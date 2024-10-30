import os
import logging
import pandas as pd
import yaml
from utils import transform_column_names

logger = logging.getLogger("EnergyDataProcessor")
logging.basicConfig(level=logging.INFO)

def load_codebook():
    # Load the original codebook
    codebook_df = pd.read_csv('owid-energy-codebook.csv')
    # Replace 'terawatt' with 'kilowatt' in 'unit' column
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)terawatt', 'kilowatt', regex=True)
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
    # Assuming owid-energy-data.csv has been downloaded
    df = pd.read_csv('owid-energy-data.csv')
    return df

def filter_main_dataset(df, config):
    # Keep only the columns specified in config['columns_to_keep']
    df_filtered = df[config['columns_to_keep']].copy()
    # Drop rows with missing population or electricity_demand
    df_filtered.dropna(subset=['population', 'electricity_demand'], inplace=True)
    return df_filtered

def apply_unit_conversion(df, codebook_df):
    # Convert columns with 'kilowatt-hours' units
    for idx, row in codebook_df.iterrows():
        col = row['column']
        unit = row['unit']
        if col in df.columns and isinstance(unit, str):
            if 'terawatt-hours' in unit.lower():
                df[col] = df[col] * 1e9  # Convert TWh to kWh
                logger.info(f"Converted {col} from TWh to kWh")
    return df

def filter_year_range(df, config):
    active_year = config['active_year']
    previous_year_range = config['previousYearRange']
    start_year = active_year - previous_year_range
    df_filtered = df[df['year'] >= start_year]
    return df_filtered

def prioritize_active_year(df, config):
    active_year = config['active_year']
    df = df.sort_values(['country', 'iso_code', 'year'], ascending=[True, True, False])
    df_latest = df.groupby(['country', 'iso_code']).first().reset_index()
    return df_latest

def fill_gdp_using_world_bank(df_latest, config):
    # Implement the logic to fill missing GDP values using World Bank data
    # For simplicity, assume this function fills missing GDP values
    return df_latest

def attach_units_to_df(df_latest, codebook_df):
    # Create a mapping from original column names to units
    unit_map = dict(zip(codebook_df['column'], codebook_df['unit']))
    # Store units in df_latest as an attribute
    df_latest.units = [unit_map.get(col, None) for col in df_latest.columns]
    return df_latest

def rename_columns(df_latest, codebook_df):
    # Transform codebook_df to update the column names and units
    transformed_codebook = transform_column_names(codebook_df.copy(), is_codebook=True)
    # Create a mapping from original column names to transformed column names
    rename_map = dict(zip(codebook_df['column'], transformed_codebook['column']))
    # Rename columns in df_latest using the mapping
    df_latest.rename(columns=rename_map, inplace=True)
    return df_latest

def main():
    logger.info("Starting energy data processing script.")
    # Load datasets
    codebook_df = load_codebook()
    config = load_or_create_config()
    df = load_main_dataset()

    # Apply transformations and filters
    df_filtered = filter_main_dataset(df, config)
    df_filtered = apply_unit_conversion(df_filtered, codebook_df)
    df_filtered = filter_year_range(df_filtered, config)
    df_latest = prioritize_active_year(df_filtered, config)
    df_latest = fill_gdp_using_world_bank(df_latest, config)

    # Attach units to df_latest
    df_latest = attach_units_to_df(df_latest, codebook_df)

    # Rename columns
    df_latest = rename_columns(df_latest, codebook_df)

    # Save the processed dataset
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

if __name__ == "__main__":
    main()
