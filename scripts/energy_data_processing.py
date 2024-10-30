import os
import logging
import pandas as pd
import yaml
import requests
import threading
from utils import transform_column_names, apply_unit_conversion, apply_transformations

logger = logging.getLogger("EnergyDataProcessor")
logging.basicConfig(level=logging.INFO)

def download_datasets():
    # Unchanged code
    # ...

def load_codebook():
    codebook_df = pd.read_csv('owid-energy-codebook.csv')
    return codebook_df

def load_or_create_config():
    # Unchanged code
    # ...

def load_main_dataset():
    df = pd.read_csv('owid-energy-data.csv')
    return df

def filter_main_dataset(df, config):
    # Unchanged code
    # ...

def filter_year_range(df, config):
    # Unchanged code
    # ...

def prioritize_active_year(df, config):
    # Unchanged code
    # ...

def fetch_gdp_data_range(country_code, start_year, end_year, result_dict, retry=3):
    # Unchanged code
    # ...

def fill_gdp_using_world_bank(df, active_year, previousYearRange):
    # Unchanged code
    # ...

def round_numeric_columns(df):
    # Updated function as shown in step 4
    # ...

def attach_units_to_df(df_latest, codebook_df):
    # Unchanged code
    # ...

def rename_columns(df_latest, codebook_df):
    # Unchanged code
    # ...

def convert_percentages_to_fractions(df, codebook_df):
    # Unchanged code
    # ...

def main():
    logger.info("Starting energy data processing script.")
    # Download datasets
    download_datasets()
    # Load datasets
    codebook_df = load_codebook()
    config = load_or_create_config()
    df = load_main_dataset()

    # Apply transformations to codebook_df
    codebook_df = apply_transformations(codebook_df)

    # Apply transformations and filters
    df_filtered = filter_main_dataset(df, config)
    # Apply unit conversions and update codebook_df
    df_filtered, codebook_df = apply_unit_conversion(df_filtered, codebook_df)
    # Attach units to df_filtered after unit conversion
    df_filtered = attach_units_to_df(df_filtered, codebook_df)
    df_filtered, codebook_df = convert_percentages_to_fractions(df_filtered, codebook_df)
    df_filtered = filter_year_range(df_filtered, config)
    df_latest = prioritize_active_year(df_filtered, config)

    # Fill missing GDP data
    df_latest = fill_gdp_using_world_bank(df_latest, config['active_year'], config['previousYearRange'])
    # Round specified numeric columns
    df_latest = round_numeric_columns(df_latest)

    # Units are already attached to df_latest
    # Rename columns
    df_latest, codebook_df = rename_columns(df_latest, codebook_df)

    # Save the processed dataset
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

if __name__ == "__main__":
    main()
