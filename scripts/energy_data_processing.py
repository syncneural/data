# energy_data_processing.py

import os
import logging
import pandas as pd
import yaml
import requests
import threading
from utils import transform_column_names, apply_unit_conversion

logger = logging.getLogger("EnergyDataProcessor")
logging.basicConfig(level=logging.INFO)

def download_datasets():
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
    df_filtered = df[config['columns_to_keep']].copy()
    df_filtered.dropna(subset=['population'], inplace=True)
    return df_filtered

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

def fetch_gdp_data_range(country_code, start_year, end_year, result_dict):
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

    for thread in threads:
        thread.join()

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

def apply_unit_conversion_script(df, codebook_df):
    """
    Applies unit conversions to the DataFrame based on the codebook's unit definitions.
    This function does NOT modify the codebook.
    """
    df = apply_unit_conversion(df, codebook_df)
    return df

def update_codebook_units_after_conversion(codebook_df):
    """
    Update the unit field in the codebook_df based on the conversions applied.
    """
    for idx, row in codebook_df.iterrows():
        unit = row['unit']
        if isinstance(unit, str):
            normalized_unit = unit.lower()
            if 'terawatt-hour' in normalized_unit or 'twh' in normalized_unit:
                codebook_df.at[idx, 'unit'] = 'kilowatt-hours'
                logger.info(f"Updated unit for '{row['column']}' to 'kilowatt-hours'")
    return codebook_df

def round_numeric_columns(df):
    columns_to_round_0 = ['population', 'gdp']

    kwh_columns = [col for col in df.columns if 'kwh' in col.lower()]
    carbon_intensity_columns = [col for col in df.columns if 'gco₂e/kwh' in col.lower() or 'gco2e/kwh' in col.lower()]

    columns_to_round_0.extend(kwh_columns)
    columns_to_round_0.extend(carbon_intensity_columns)
    columns_to_round_0 = list(set(columns_to_round_0))

    for col in columns_to_round_0:
        if col in df.columns:
            df[col] = df[col].round(0).astype('Int64')
            logger.info(f"Rounded '{col}' to zero decimal places.")
    return df

def rename_columns(df_latest, codebook_df):
    # Ensure 'latest_data_year' is in codebook
    if 'latest_data_year' not in codebook_df['column'].values:
        new_row = pd.DataFrame({
            'column': ['latest_data_year'],
            'description': ['Year of the latest data available for the country'],
            'unit': ['Year'],
            'source': ['Data processing']
        })
        codebook_df = pd.concat([codebook_df, new_row], ignore_index=True)

    # Filter codebook to columns in df_latest
    codebook_df = codebook_df[codebook_df['column'].isin(df_latest.columns)].reset_index(drop=True)

    # Transform column names
    transformed_codebook = transform_column_names(codebook_df.copy(), is_codebook=True)

    # Create rename map
    rename_map = dict(zip(codebook_df['column'], transformed_codebook['column']))

    # Rename df_latest columns
    df_latest.rename(columns=rename_map, inplace=True)
    logger.info(f"Renamed columns: {rename_map}")
    return df_latest

def main():
    logger.info("Starting energy data processing script.")
    download_datasets()
    config = load_or_create_config()

    df = load_main_dataset()

    df_filtered = filter_main_dataset(df, config)
    codebook_df = load_codebook()
    df_filtered = apply_unit_conversion_script(df_filtered, codebook_df)
    codebook_df = update_codebook_units_after_conversion(codebook_df)
    df_filtered = filter_year_range(df_filtered, config)
    df_latest = prioritize_active_year(df_filtered, config)

    df_latest = fill_gdp_using_world_bank(df_latest, config['active_year'], config['previousYearRange'])

    # **First: Rename columns to include 'kWh'**
    df_latest = rename_columns(df_latest, codebook_df)

    # **Second: Round numeric columns, now that 'kWh' is in the column names**
    df_latest = round_numeric_columns(df_latest)

    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

if __name__ == "__main__":
    main()
