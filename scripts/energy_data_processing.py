# Step 1: Import required libraries
import pandas as pd
import requests
import logging
import yaml
from utils import transform_column_names  # Import utility function for transforming column names

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EnergyDataProcessing")

# Configure pandas display settings for clarity
pd.set_option('display.float_format', '{:.0f}'.format)  # Show full numbers, no scientific notation
pd.set_option('display.max_rows', 250)  # Display up to 250 rows for clarity
pd.set_option('display.max_columns', None)  # Display all columns without truncation

# Set the active year and previous year range
active_year = 2022
previousYearRange = 5

# Step 2: Load configuration from config.yaml
# If the config file does not exist, create one from the existing columns

def load_or_create_config():
    config_path = 'config.yaml'
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.info(f"Config file not found. Creating new config.yaml with current columns.")
        columns_to_keep = [
            'country', 'iso_code', 'year', 'population', 'gdp', 'biofuel_electricity',
            'biofuel_share_elec', 'carbon_intensity_elec', 'coal_electricity', 'coal_share_elec',
            'electricity_demand', 'electricity_generation', 'electricity_share_energy',
            'fossil_elec_per_capita', 'fossil_electricity', 'fossil_share_elec', 'gas_electricity',
            'gas_share_elec', 'hydro_electricity', 'hydro_share_elec', 'low_carbon_elec_per_capita',
            'low_carbon_electricity', 'low_carbon_share_elec', 'nuclear_electricity',
            'nuclear_share_elec', 'oil_electricity', 'oil_share_elec', 'other_renewable_electricity',
            'other_renewable_exc_biofuel_electricity', 'other_renewables_elec_per_capita',
            'other_renewables_elec_per_capita_exc_biofuel', 'other_renewables_share_elec',
            'other_renewables_share_elec_exc_biofuel', 'per_capita_electricity',
            'renewables_elec_per_capita', 'renewables_electricity', 'renewables_share_elec',
            'solar_electricity', 'solar_share_elec', 'wind_electricity', 'wind_share_elec'
        ]
        config = {'columns_to_keep': columns_to_keep, 'active_year': active_year, 'previous_year_range': previousYearRange}
        with open(config_path, 'w') as file:
            yaml.dump(config, file)
    return config

# Step 3: Fetch the entire GDP data range for all countries at once
def fetch_gdp_data_range(country_code, start_year, end_year):
    """
    Fetches GDP data for a given country across a specified year range in a single API call.

    Args:
        country_code (str): The ISO 3-letter country code.
        start_year (int): The starting year for the data range.
        end_year (int): The ending year for the data range.

    Returns:
        dict: A dictionary containing GDP data for each year in the range, with year as the key and GDP value as the value.
    """
    year_range = f"{start_year}:{end_year}"
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?date={year_range}&format=json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        gdp_data = {}
        if len(data) > 1:
            for item in data[1]:
                gdp_value = item['value']
                year = int(item['date'])
                gdp_data[year] = gdp_value
        return gdp_data
    else:
        logger.error(f"Failed to fetch GDP data for {country_code} from World Bank API, status code: {response.status_code}")
        return None

# Step 4: Load the main dataset
def load_main_dataset():
    return pd.read_csv('owid-energy-data.csv', delimiter=',')

# Step 5: Filter for relevant columns and drop rows with missing population or electricity_demand
def filter_main_dataset(df, config):
    columns_to_keep = config['columns_to_keep']
    df_filtered = df[columns_to_keep].dropna(subset=['population', 'electricity_demand'])
    return df_filtered

# Step 6: Apply codebook-based unit conversion for TWh to kWh only
def apply_unit_conversion(df_filtered, codebook_df):
    for _, row in codebook_df.iterrows():
        col_name, unit = row['column'], row['unit']
        if col_name in df_filtered.columns and isinstance(unit, str) and unit == "terawatt-hours":
            df_filtered[col_name] = df_filtered[col_name].apply(lambda x: x * 1e9 if pd.notna(x) else x)  # Convert TWh to kWh
            logger.info(f"Converted {col_name} from TWh to kWh")
    return df_filtered

# Step 7: Filter for data within the specified year range and set 'latest_data_year'
def filter_year_range(df_filtered, active_year, previousYearRange):
    df_filtered = df_filtered[(df_filtered['year'] >= active_year - previousYearRange) & (df_filtered['year'] <= active_year)]
    df_filtered['latest_data_year'] = df_filtered['year']
    return df_filtered

# Step 8: Prioritize data for the active year, if available; otherwise, use the latest year within the range
def prioritize_active_year(df_filtered):
    df_filtered = df_filtered.sort_values(by=['country', 'iso_code', 'year'], ascending=[True, True, False])
    df_latest = df_filtered.groupby(['country', 'iso_code']).first().reset_index()
    return df_latest

# Step 9: Fill missing GDP data using the optimized approach
def fill_gdp_using_world_bank(df, active_year, previousYearRange):
    missing_gdp = df[df['gdp'].isna() & df['iso_code'].notna()]

    for index, row in missing_gdp.iterrows():
        country_code = row['iso_code']
        gdp_data = fetch_gdp_data_range(country_code, active_year - previousYearRange, active_year)
        if gdp_data:
            latest_year = max(gdp_data.keys())
            df.at[index, 'gdp'] = gdp_data[latest_year]
            df.at[index, 'latest_data_year'] = latest_year
            logger.info(f"Filled GDP for {row['country']} using year: {latest_year}")
        else:
            logger.warning(f"No GDP data available within the specified range for {row['country']}")
    return df

# Step 10: Rename columns as the final step using the utility function
def rename_columns(df_latest, codebook_df):
    # Properly pass the entire DataFrame and codebook to the transform function
    return transform_column_names(df_latest, codebook_df)

# Step 11: Main function
def main():
    config = load_or_create_config()
    df = load_main_dataset()
    df_filtered = filter_main_dataset(df, config)
    codebook_df = pd.read_csv('owid-energy-codebook.csv')  # Load codebook if not already loaded
    df_filtered = apply_unit_conversion(df_filtered, codebook_df)
    df_filtered = filter_year_range(df_filtered, config['active_year'], config['previous_year_range'])
    df_latest = prioritize_active_year(df_filtered)
    df_latest = fill_gdp_using_world_bank(df_latest, config['active_year'], config['previous_year_range'])
    df_latest = rename_columns(df_latest, codebook_df)

    # Save the processed dataset
    df_latest.to_csv('output/processed_energy_data.csv', index=False)
    logger.info("Processed energy data saved to output/processed_energy_data.csv")

# Run main function
if __name__ == "__main__":
    main()
