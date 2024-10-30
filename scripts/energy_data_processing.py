import os
import logging
import pandas as pd
import yaml
import requests
import threading
from utils import transform_column_names

logger = logging.getLogger("EnergyDataProcessor")
logging.basicConfig(level=logging.INFO)

def download_datasets():
    # Download datasets if they don't exist or if force_update is True
    if not os.path.exists('owid-energy-data.csv'):
        data_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'
        df = pd.read_csv(data_url)
        df.to_csv('owid-energy-data.csv', index=False)
        logger.info("Downloaded owid-energy-data.csv successfully.")
    else:
        logger.info("owid-energy-data.csv already exists.")

    if not os.path.exists('owid-energy-codebook.csv'):
        codebook_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-codebook.csv'
        codebook_df = pd.read_csv(codebook_url)
        codebook_df.to_csv('owid-energy-codebook.csv', index=False)
        logger.info("Downloaded owid-energy-codebook.csv successfully.")
    else:
        logger.info("owid-energy-codebook.csv already exists.")

def load_codebook():
    codebook_df = pd.read_csv('owid-energy-codebook.csv')
    return codebook_df

def load_or_create_config():
    config_path = 'config.yaml'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        # Default configuration if config.yaml does not exist
        config = {
            'columns_to_keep': ['country', 'year', 'iso_code', 'population', 'gdp'],
            'active_year': 2022,
            'previousYearRange': 5,
            'force_update': True
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
            logger.info(f"Created default configuration at {config_path}")
    return config

def load_main_dataset():
    df = pd.read_csv('owid-energy-data.csv')
    return df

def filter_main_dataset(df, config):
    # Keep only the columns specified in config['columns_to_keep']
    df_filtered = df[config['columns_to_keep']].copy()
    # Drop rows with missing population
    df_filtered.dropna(subset=['population'], inplace=True)
    return df_filtered

def apply_unit_conversion(df, codebook_df):
    # Convert units based on codebook
    for idx, row in codebook_df.iterrows():
        col = row['column']
        unit = row['unit']
        if col in df.columns and isinstance(unit, str):
            if 'terawatt-hours' in unit.lower():
                df[col] = df[col] * 1e9  # Convert TWh to kWh
                # Update the unit in the codebook
                codebook_df.at[idx, 'unit'] = unit.lower().replace('terawatt-hours', 'kilowatt-hours')
                logger.info(f"Converted {col} from TWh to kWh and updated unit in codebook.")
            elif 'million tonnes' in unit.lower():
                df[col] = df[col] * 1e6  # Convert million tonnes to tonnes
                codebook_df.at[idx, 'unit'] = unit.lower().replace('million tonnes', 'tonnes')
                logger.info(f"Converted {col} from million tonnes to tonnes and updated unit in codebook.")
            # Add other unit conversions as needed
    return df, codebook_df

def convert_percentages_to_fractions(df, codebook_df):
    # Identify columns with '%' in their units
    percentage_columns = codebook_df[codebook_df['unit'].str.contains('%', na=False)]['column'].tolist()
    for col in percentage_columns:
        if col in df.columns:
            df[col] = df[col] / 100.0  # Convert percentage to fraction
            # Update the unit in the codebook
            idx = codebook_df[codebook_df['column'] == col].index[0]
            codebook_df.at[idx, 'unit'] = 'fraction'
            logger.info(f"Converted {col} from percentage to fraction and updated unit in codebook.")
    return df, codebook_df

def filter_year_range(df, config):
    active_year = config['active_year']
    previous_year_range = config['previousYearRange']
    start_year = active_year - previous_year_range
    df_filtered = df[df['year'] >= start_year]
    return df_filtered

def prioritize_active_year(df, config):
    df = df.sort_values(['country', 'iso_code', 'year'], ascending=[True, True, False])
    df_latest = df.groupby(['country', 'iso_code']).first().reset_index()
    df_latest['latest_data_year'] = df_latest['year']
    return df_latest

def fetch_gdp_data_range(country_code, start_year, end_year, result_dict, retry=3):
    """
    Fetches GDP data for a given country across a specified year range in a single API call.
    """
    year_range = f"{start_year}:{end_year}"
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?date={year_range}&format=json"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            gdp_data = {}
            if len(data) > 1 and data[1]:
                for item in data[1]:
                    gdp_value = item['value']
                    year = int(item['date'])
                    if gdp_value is not None:
                        gdp_data[year] = gdp_value
                result_dict[country_code] = gdp_data
            else:
                logger.error(f"No GDP data found for {country_code}")
                result_dict[country_code] = None
        else:
            logger.error(f"Failed to fetch GDP data for {country_code} from World Bank API, status code: {response.status_code}")
            result_dict[country_code] = None
    except Exception as e:
        logger.error(f"Exception occurred while fetching GDP data for {country_code}: {e}")
        result_dict[country_code] = None

def fill_gdp_using_world_bank(df, active_year, previousYearRange):
    missing_gdp = df[df['gdp'].isna() & df['iso_code'].notna()]
    threads = []
    gdp_results = {}

    for index, row in missing_gdp.iterrows():
        country_code = row['iso_code']
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
            df.loc[index, 'gdp'] = gdp_data[latest_year]
            df.loc[index, 'latest_data_year'] = latest_year
            logger.info(f"Filled GDP for {row['country']} using year: {latest_year}")
        else:
            logger.warning(f"No GDP data available within the specified range for {row['country']}")
    return df

def calculate_per_capita_metrics(df):
    # Calculate per capita values for energy consumption columns
    energy_columns = [col for col in df.columns if 'consumption' in col.lower()]
    for col in energy_columns:
        per_capita_col = f"{col}_per_capita"
        df[per_capita_col] = df[col] / df['population']
        logger.info(f"Calculated {per_capita_col}")
    return df

def attach_units_to_df(df_latest, codebook_df):
    # Create a mapping from original column names to units
    unit_map = dict(zip(codebook_df['column'], codebook_df['unit']))
    # Store units in df_latest as an attribute
    df_latest.units = [unit_map.get(col, None) for col in df_latest.columns]
    return df_latest

def rename_columns(df_latest, codebook_df):
    # Add 'latest_data_year' to codebook if not present
    if 'latest_data_year' not in codebook_df['column'].values:
        new_row = pd.DataFrame({
            'column': ['latest_data_year'],
            'description': ['Year of the latest data available for the country'],
            'unit': ['Year'],
            'source': ['Data processing']
        })
        codebook_df = pd.concat([codebook_df, new_row], ignore_index=True)

    # Transform codebook_df to update the column names and units
    transformed_codebook = transform_column_names(codebook_df.copy(), is_codebook=True)

    # Create a mapping from original column names to transformed column names
    rename_map = dict(zip(codebook_df['column'], transformed_codebook['column']))

    # Rename columns in df_latest using the mapping
    df_latest.rename(columns=rename_map, inplace=True)
    return df_latest, codebook_df

def main():
    logger.info("Starting energy data processing script.")
    # Download datasets
    download_datasets()
    # Load datasets
    codebook_df = load_codebook()
    config = load_or_create_config()
    df = load_main_dataset()

    # Apply transformations and filters
    df_filtered = filter_main_dataset(df, config)
    df_filtered, codebook_df = apply_unit_conversion(df_filtered, codebook_df)
    df_filtered, codebook_df = convert_percentages_to_fractions(df_filtered, codebook_df)
    df_filtered = filter_year_range(df_filtered, config)
    df_latest = prioritize_active_year(df_filtered, config)

    # Fill missing GDP data using the multithreaded approach
    df_latest = fill_gdp_using_world_bank(df_latest, config['active_year'], config['previousYearRange'])
    df_latest = calculate_per_capita_metrics(df_latest)

    # Attach units to df_latest
    df_latest = attach_units_to_df(df_latest, codebook_df)

    # Rename columns
    df_latest, codebook_df = rename_columns(df_latest, codebook_df)

    # Save the processed dataset
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

if __name__ == "__main__":
    main()
