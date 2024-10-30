# Step 1: Import required libraries
import threading
import time
import pandas as pd
import requests
import logging
import yaml
import os
from utils import transform_column_names  # Import utility function for transforming column names

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EnergyDataProcessing")

# Configure pandas display settings for clarity
pd.set_option('display.float_format', '{:.0f}'.format)  # Show full numbers, no scientific notation
pd.set_option('display.max_rows', 250)  # Display up to 250 rows for clarity
pd.set_option('display.max_columns', None)  # Display all columns without truncation


# Step 2: Load configuration from config.yaml
# If the config file does not exist, create one with the relevant keys
def load_or_create_config():
    config_path = 'config.yaml'
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            # Ensure all keys are present in the loaded config
            config.setdefault('previous_year_range', config.get('previous_year_range', 5))
            config.setdefault('active_year', config.get('active_year', 2022))
            config.setdefault('main_columns_to_keep', [])
            config.setdefault('force_update', True)
            config.setdefault('timeline_year_range', {'start_year': 2000, 'end_year': 2022})
            config.setdefault('timeline_main_columns_to_keep', [])
    except FileNotFoundError:
        logger.info(f"Config file not found. Creating new config.yaml with default settings.")
        config = {
            'main_columns_to_keep': [],
            'active_year': 2022,
            'previous_year_range': 5,
            'force_update': True,
            'timeline_year_range': {'start_year': 2000, 'end_year': 2022},
            'timeline_main_columns_to_keep': []
        }
        with open(config_path, 'w') as file:
            yaml.dump(config, file)
    return config

# Step 3: Fetch the entire GDP data range for all countries at once
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
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        gdp_data = {}
        if len(data) > 1:
            for item in data[1]:
                gdp_value = item['value']
                year = int(item['date'])
                gdp_data[year] = gdp_value
        result_dict[country_code] = gdp_data
    else:
        logger.error(f"Failed to fetch GDP data for {country_code} from World Bank API, status code: {response.status_code}")
        result_dict[country_code] = None

import urllib.request

# Step 4: Download the datasets concurrently
def download_datasets(config):
    force_update = config.get('force_update', True)
    urls = [
        ("owid-energy-data.csv", "https://raw.githubusercontent.com/owid/energy-data/refs/heads/master/owid-energy-data.csv"),
        ("owid-energy-codebook.csv", "https://raw.githubusercontent.com/owid/energy-data/refs/heads/master/owid-energy-codebook.csv")
    ]

    threads = []
    for filename, url in urls:
        if force_update or not os.path.exists(filename):
            thread = threading.Thread(target=download_file, args=(filename, url))
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()

def download_file(filename, url):
    try:
        response = requests.get(url)
        with open(filename, "wb") as f:
            f.write(response.content)
        logger.info(f"Downloaded {filename} successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {filename}: {e}")

# Step 5: Load the main dataset
def load_main_dataset():
    return pd.read_csv('owid-energy-data.csv', delimiter=',')

# Step 6: Filter for relevant columns and drop rows with missing population or electricity_demand
def filter_dataset(df, columns_required):
    if not columns_required:
        raise ValueError(f"'{columns_required}' in config.yaml is empty. Please specify the correct columns to keep.")
    df_filtered = df[columns_required].dropna(subset=['population', 'electricity_demand'])
    return df_filtered

# Step 7: Apply codebook-based unit conversion for TWh to kWh only
def apply_unit_conversion(df_filtered, codebook_df):
    for _, row in codebook_df.iterrows():
        col_name, unit = row['column'], row['unit']
        if col_name in df_filtered.columns and isinstance(unit, str) and unit == "terawatt-hours":
            df_filtered[col_name] = df_filtered[col_name].apply(lambda x: x * 1e9 if pd.notna(x) else x)  # Convert TWh to kWh
            logger.info(f"Converted {col_name} from TWh to kWh")
    return df_filtered

# Step 8: Filter for data within the specified year range and set 'latest_data_year'
def filter_year_range(df_filtered, start_year, end_year):
    df_filtered = df_filtered[(df_filtered['year'] >= start_year) & (df_filtered['year'] <= end_year)]
    df_filtered['latest_data_year'] = df_filtered['year']
    return df_filtered

# Step 9: Prioritize data for the active year, if available; otherwise, use the latest year within the range
def prioritize_active_year(df_filtered):
    df_filtered = df_filtered.sort_values(by=['country', 'iso_code', 'year'], ascending=[True, True, False])
    df_latest = df_filtered.groupby(['country', 'iso_code']).first().reset_index()
    return df_latest

# Step 10: Fill missing GDP data using a multithreaded approach
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
            df.loc[index, 'gdp'] = gdp_data[latest_year]  # Used .loc[] to ensure correct DataFrame modification
            df.loc[index, 'latest_data_year'] = latest_year
            logger.info(f"Filled GDP for {row['country']} using year: {latest_year}")
        else:
            logger.warning(f"No GDP data available within the specified range for {row['country']}")
    return df

# Step 11: Rename columns as the final step using the utility function
def rename_columns(df, codebook_df):
    # Transform codebook_df first to update the column names
    transformed_codebook = transform_column_names(codebook_df)
    
    # Now apply the renamed columns from transformed_codebook to df
    rename_map = dict(zip(codebook_df['column'], transformed_codebook['column']))
    logger.info(f"Column rename mapping: {rename_map}")  # Add logging for rename map
    
    df.rename(columns=rename_map, inplace=True)
    return df

# Step 12: Generate and save the output datasets
def save_output_datasets(df, codebook_df, config, output_dir):
    # Generate the main dataset
    df_latest = prioritize_active_year(df)
    df_latest = fill_gdp_using_world_bank(df_latest, config['active_year'], config['previous_year_range'])
    df_latest = rename_columns(df_latest, codebook_df)
    output_path_main = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.to_csv(output_path_main, index=False)
    logger.info(f"Processed energy data saved to {output_path_main}")

    # Generate the additional timeline dataset for broader year range
    start_year = config['timeline_year_range']['start_year']
    end_year = config['timeline_year_range']['end_year']
    columns_required = config['timeline_main_columns_to_keep']
    df_filtered_year_range = filter_year_range(df, start_year, end_year)
    df_filtered_year_range = filter_dataset(df_filtered_year_range, columns_required)
    df_filtered_year_range = rename_columns(df_filtered_year_range, codebook_df)
    output_path_timeline = os.path.join(output_dir, f'processed_energy_data_{start_year}_{end_year}.csv')
    df_filtered_year_range.to_csv(output_path_timeline, index=False)
    logger.info(f"Processed energy timeline data saved to {output_path_timeline}")

# Step 13: Main function
def main():
    config = load_or_create_config()
    active_year = config['active_year']
    previousYearRange = config['previous_year_range']
    # Download the datasets, decide whether to force update based on config
    download_datasets(config)
    
    # Ensure the output directory exists
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df = load_main_dataset()
    df_filtered = filter_dataset(df, config['main_columns_to_keep'])
    codebook_df = pd.read_csv('owid-energy-codebook.csv')  # Load codebook if not already loaded
    df_filtered = apply_unit_conversion(df_filtered, codebook_df)
    
    # Apply year filtering for the main dataset
    df_filtered = filter_year_range(df_filtered, active_year - previousYearRange, active_year)
    
    # Generate and save the output datasets
    save_output_datasets(df_filtered, codebook_df, config, output_dir)

# Run main function
if __name__ == "__main__":
    main()
