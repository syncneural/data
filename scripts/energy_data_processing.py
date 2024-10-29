# Step 1: Import required libraries
import pandas as pd
import requests
import logging
import threading
import time
import os
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EnergyDataProcessor")  # Set logger name to identify messages from this module

# Step 2: Configure pandas display settings for clarity
def configure_pandas():
    pd.set_option('display.float_format', '{:.0f}'.format)  # Show full numbers, no scientific notation
    pd.set_option('display.max_rows', 250)  # Display up to 5 rows for brevity
    pd.set_option('display.max_columns', None)  # Display all columns without truncation

# Step 3: Load configuration from config.yaml
def load_config():
    config_path = 'config.yaml'
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("Config file not found. Please make sure config.yaml is available.")
        raise
    return config

# Set the active year and previous year range from the configuration
config = load_config()
active_year = config['active_year']
previousYearRange = config['previousYearRange']

# Step 4: Pre-cache GDP data if it already exists
def load_cached_gdp_data():
    if os.path.exists("cached_gdp_data.csv"):
        return pd.read_csv("cached_gdp_data.csv", index_col=0).to_dict(orient='index')
    else:
        return {}

# Step 5: Save cached GDP data
def save_cached_gdp_data(gdp_data):
    pd.DataFrame.from_dict(gdp_data, orient='index').to_csv("cached_gdp_data.csv")

# Step 6: Fetch the GDP data for a specific country
# Added backoff strategy for retrying requests
def fetch_gdp_data_range(country_code, start_year, end_year, result_dict, retry=3):
    """
    Fetches GDP data for a given country across a specified year range in a single API call.

    Args:
        country_code (str): The ISO 3-letter country code.
        start_year (int): The starting year for the data range.
        end_year (int): The ending year for the data range.
        result_dict (dict): A dictionary to store the fetched GDP data.
        retry (int): Number of retries in case of failure.
    """
    year_range = f"{start_year}:{end_year}"
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?date={year_range}&format=json"
    backoff_time = 5
    while retry > 0:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            gdp_data = {}
            if len(data) > 1:
                for item in data[1]:
                    gdp_value = item['value']
                    year = int(item['date'])
                    gdp_data[year] = gdp_value
            result_dict[country_code] = gdp_data
            return
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch GDP data for {country_code} from World Bank API: {e}. Retries left: {retry-1}")
            retry -= 1
            time.sleep(backoff_time)  # Implement exponential backoff
            backoff_time *= 2
    result_dict[country_code] = None

# Step 7: Download the main dataset and codebook if necessary
# Added parallel download functionality
def download_datasets():
    urls = [
        ("owid-energy-data.csv", "https://raw.githubusercontent.com/owid/energy-data/refs/heads/master/owid-energy-data.csv"),
        ("owid-energy-codebook.csv", "https://raw.githubusercontent.com/owid/energy-data/refs/heads/master/owid-energy-codebook.csv")
    ]

    threads = []
    for filename, url in urls:
        thread = threading.Thread(target=download_file, args=(filename, url))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

# Step 8: Function to download a file
def download_file(filename, url):
    try:
        response = requests.get(url)
        with open(filename, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded {filename} successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {filename}: {e}")

# Step 9: Load the main dataset
def load_main_dataset():
    return pd.read_csv('owid-energy-data.csv', delimiter=',')

# Step 10: Load or update configuration, filter for relevant columns, and drop rows with missing population or electricity_demand
def filter_relevant_columns(df):
    config = load_config()
    columns_to_keep = config['columns_to_keep']
    df_filtered = df[columns_to_keep].dropna(subset=['population', 'electricity_demand'])
    # Dropping rows with missing population or electricity demand as countries without these values are not relevant to this dataset.
    return df_filtered

# Step 11: Apply codebook-based unit conversion for TWh to kWh and percentages
def apply_unit_conversion(df_filtered, codebook_df):
    for _, row in codebook_df.iterrows():
        col_name, unit = row['column'], row['unit']
        if col_name in df_filtered.columns and isinstance(unit, str):
            if unit == "terawatt-hours":
                df_filtered[col_name] = df_filtered[col_name].apply(lambda x: x * 1e9 if pd.notna(x) else x)  # Convert TWh to kWh
                logger.info(f"Converted {col_name} from TWh to kWh")
            elif unit == "%":
                df_filtered[col_name] = df_filtered[col_name].apply(lambda x: x / 100 if pd.notna(x) else x)  # Convert percentage to fraction
                logger.info(f"Converted {col_name} from % to fraction")
    return df_filtered

# Step 12: Filter for data within the specified year range
def filter_year_range(df_filtered):
    df_filtered = df_filtered[(df_filtered['year'] >= active_year - previousYearRange) & (df_filtered['year'] <= active_year)]
    df_filtered.loc[:, 'latest_data_year'] = df_filtered['year']  # Avoided SettingWithCopyWarning by using .loc[]
    return df_filtered

# Step 13: Prioritize data for the active year, if available; otherwise, use the latest year within the range
def prioritize_active_year(df_filtered):
    df_filtered = df_filtered.sort_values(by=['country', 'iso_code', 'year'], ascending=[True, True, False])
    return df_filtered.groupby(['country', 'iso_code']).first().reset_index()

# Step 14: Fill missing GDP data using the optimized approach
# Now includes pre-caching of GDP data
def fill_gdp_using_world_bank(df, gdp_results):
    missing_gdp = df[df['gdp'].isna() & df['iso_code'].notna()]
    threads = []
    for index, row in missing_gdp.iterrows():
        country_code = row['iso_code']
        if country_code in gdp_results:  # Use cached data if available
            continue
        thread = threading.Thread(target=fetch_gdp_data_range, args=(country_code, active_year - previousYearRange, active_year, gdp_results))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Fill the GDP data from the results
    for index, row in missing_gdp.iterrows():
        country_code = row['iso_code']
        gdp_data = gdp_results.get(country_code, None)
        if gdp_data:
            latest_year = max(gdp_data.keys())
            df.loc[index, 'gdp'] = gdp_data[latest_year]  # Used .loc[] to ensure correct DataFrame modification
            df.loc[index, 'latest_data_year'] = latest_year  # Used .loc[] to ensure correct DataFrame modification
            logger.info(f"Filled GDP for {row['country']} using year: {latest_year}")
        else:
            logger.warning(f"No GDP data available within the specified range for {row['country']}")
    return df

# Step 15: Rename columns using codebook at the very end
def rename_columns(df_latest, codebook_df):
    rename_map = {}
    for _, row in codebook_df.iterrows():
        col_name, unit = row['column'], row['unit']
        if col_name in df_latest.columns:
            new_col_name = col_name.replace("_", " ").title()
            # Append unit to the column name as needed
            if isinstance(unit, str):
                normalized_unit = unit.lower().replace(" ", "").replace("co₂", "co2").strip()
                if "terawatt-hours" in normalized_unit:
                    new_col_name += " kWh"
                elif "kilowatt-hours" in normalized_unit:
                    new_col_name += " kWh"
                elif "gramsofco2equivalentsperkilowatt-hour" in normalized_unit:
                    new_col_name += " gCO₂e/kWh"
                elif "%" in normalized_unit:
                    new_col_name += " %"
                elif "international-$" in normalized_unit:
                    new_col_name += " ISD"
            rename_map[col_name] = new_col_name

    df_latest = df_latest.rename(columns=rename_map)

    essential_columns = {
        'country': 'Country',
        'iso code': 'ISO Code',
        'year': 'Year',
        'gdp isd': 'GDP ISD',
        'latest_data_year': 'Latest Data Year'
    }
    df_latest.columns = [essential_columns.get(col.lower(), col) for col in df_latest.columns]
    return df_latest

# Step 16: Main execution function
def main():
    configure_pandas()
    download_datasets()
    df = load_main_dataset()
    codebook_df = pd.read_csv('owid-energy-codebook.csv', delimiter=',')
    df_filtered = filter_relevant_columns(df)
    df_filtered = apply_unit_conversion(df_filtered, codebook_df)
    df_filtered = filter_year_range(df_filtered)
    df_latest = prioritize_active_year(df_filtered)

    # Start filling missing GDP data in parallel for each country
    gdp_results = load_cached_gdp_data()
    df_latest = fill_gdp_using_world_bank(df_latest, gdp_results)
    save_cached_gdp_data(gdp_results)

    # Rename columns using codebook
    df_latest = rename_columns(df_latest, codebook_df)

    # Save the final DataFrame
    output_path = 'output/processed_energy_data.csv'
    df_latest.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

# Run main function
if __name__ == "__main__":
    main()
