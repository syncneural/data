# proc.py

import os
import logging
import pandas as pd
import yaml
import re
import requests
import threading

# Setting up logging
logger = logging.getLogger("CombinedScript")
logging.basicConfig(level=logging.INFO)

# Load or create codebook function
def load_codebook():
    if not os.path.exists('owid-energy-codebook.csv'):
        codebook_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-codebook.csv'
        codebook_df = pd.read_csv(codebook_url)
        codebook_df.to_csv('owid-energy-codebook.csv', index=False)
        logger.info("Downloaded owid-energy-codebook.csv successfully.")
    else:
        logger.info("Loading existing owid-energy-codebook.csv.")
    return pd.read_csv('owid-energy-codebook.csv')

# Load or create configuration function
def load_or_create_config(codebook_df):
    config_path = 'config.yaml'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        columns_to_keep = codebook_df['column'].tolist()
        config = {
            'columns_to_keep': columns_to_keep,
            'active_year': 2022,
            'previousYearRange': 5,
            'force_update': True
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
            logger.info(f"Created default configuration at {config_path}")
    return config

# Filter codebook function
def filter_codebook(codebook_df, config):
    filtered_codebook = codebook_df[codebook_df['column'].isin(config['columns_to_keep'])].copy()
    return filtered_codebook

# Sync codebook columns with processed energy data
def sync_codebook_columns(filtered_codebook):
    processed_data = pd.read_csv('output/processed_energy_data.csv')
    transformed_columns = processed_data.columns.tolist()

    codebook_columns = filtered_codebook['column'].tolist()
    new_columns = [col for col in transformed_columns if col not in codebook_columns]

    for col in new_columns:
        filtered_codebook = pd.concat([filtered_codebook, pd.DataFrame({
            'column': [col],
            'description': ['Derived metric'],
            'unit': [''],
            'source': ['Calculated']
        })], ignore_index=True)

    filtered_codebook = filtered_codebook.set_index('column').reindex(transformed_columns).reset_index()
    filtered_codebook['description'] = filtered_codebook['description'].fillna('No description available')
    filtered_codebook['unit'] = filtered_codebook['unit'].fillna('')

    return filtered_codebook

# Save filtered codebook function
def save_filtered_codebook(filtered_codebook):
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'codebook.csv')
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

# Download datasets using threading
def download_file(url, output_path):
    if not os.path.exists(output_path):
        df = pd.read_csv(url)
        df.to_csv(output_path, index=False)
        logger.info(f"Downloaded {output_path} successfully.")
    else:
        logger.info(f"{output_path} already exists.")

def download_datasets():
    threads = []
    datasets = [
        ('https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv', 'owid-energy-data.csv'),
        ('https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-codebook.csv', 'owid-energy-codebook.csv')
    ]

    for url, output_path in datasets:
        thread = threading.Thread(target=download_file, args=(url, output_path))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

# Apply unit conversions based on the codebook
def apply_unit_conversions(df, codebook_df):
    for idx, row in codebook_df.iterrows():
        col = row['column']
        unit = row['unit']
        if col in df.columns and isinstance(unit, str):
            normalized_unit = unit.lower()
            if 'terawatt-hour' in normalized_unit or 'twh' in normalized_unit:
                df[col] = df[col] * 1e9  # Convert TWh to kWh
                logger.info(f"Converted '{col}' from TWh to kWh")
            elif 'million tonne' in normalized_unit or 'million tonnes' in normalized_unit:
                df[col] = df[col] * 1e6  # Convert million tonnes to tonnes
                logger.info(f"Converted '{col}' from million tonnes to tonnes")
            else:
                logger.info(f"No conversion needed for '{col}' with unit '{unit}'")
    return df

# Apply transformations to column names
def transform_column_names(df, is_codebook=False):
    if is_codebook:
        columns = df['column'].tolist()
        units = df['unit'].tolist()
    else:
        columns = df.columns.tolist()
        units = [None] * len(columns)  # No units in df

    new_columns = []
    for idx, col_name in enumerate(columns):
        original_col_name = col_name
        new_col_name = col_name.replace("_", " ").title()

        replacements = {
            'Iso Code': 'ISO Code',
            'Gdp': 'GDP',
            'Co2': 'CO₂',
            'Co2e': 'CO₂e',
            'Gco₂e': 'gCO₂e',
            'Kwh': 'kWh',
            'Latest Data Year': 'Latest Data Year'
        }
        for old, new in replacements.items():
            new_col_name = new_col_name.replace(old, new)

        unit = units[idx] if units else None
        if isinstance(unit, str) and unit.strip():
            normalized_unit = re.sub(r'\s+', '', unit.lower()).strip()
            normalized_unit = normalized_unit.replace('co₂', 'co2')

            if 'kilowatt-hours' in normalized_unit:
                new_col_name += ' kWh'
            elif 'gco2e/kwh' in normalized_unit or 'gramsofco2equivalentsperkilowatt-hour' in normalized_unit:
                new_col_name += ' gCO₂e/kWh'
            elif '%' in normalized_unit:
                new_col_name += ' %'
            elif 'international-$' in normalized_unit:
                new_col_name += ' ISD'
            elif 'tonnes' in normalized_unit:
                new_col_name += ' tonnes'
            elif 'year' in normalized_unit:
                pass  # Do not append unit to year columns

        new_columns.append(new_col_name)
        logger.info(f"Transformed '{original_col_name}' to '{new_col_name}'")

    if is_codebook:
        df['column'] = new_columns
    else:
        df.columns = new_columns

    logger.info(f"Final column names: {df.columns.tolist()}")
    return df

# Apply additional transformations to the codebook
def apply_additional_transformations(codebook_df):
    codebook_df['description'] = codebook_df['description'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    logger.info("Applied transformation: Replaced 'terawatt-hours' with 'kilowatt-hours' in description and unit columns.")

    codebook_df['description'] = codebook_df['description'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    logger.info("Applied transformation: Replaced 'million tonnes' with 'tonnes' in description and unit columns.")

    percentage_columns = codebook_df[codebook_df['unit'].str.contains('%', na=False)]['column'].tolist()
    for col in percentage_columns:
        idx = codebook_df[codebook_df['column'] == col].index[0]
        original_description = codebook_df.at[idx, 'description']
        if "Measured as a percentage fraction of 1" not in original_description:
            updated_description = re.sub(r'Measured as a percentage', '', original_description, flags=re.IGNORECASE).strip()
            updated_description += " (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)"
            codebook_df.at[idx, 'description'] = updated_description
            logger.info(f"Updated description for '{col}' to indicate percentage fraction.")
    return codebook_df

# Download, transform, and process energy datasets
def process_energy_data():
    download_datasets()
    df = pd.read_csv('owid-energy-data.csv')
    codebook_df = load_codebook()
    df = transform_column_names(df)
    df = apply_unit_conversions(df, codebook_df)
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

# Main processing function
def main():
    logger.info("Starting combined script.")

    # Process energy data
    process_energy_data()

    # Load codebook and configuration
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)

    # Apply transformations and filter codebook
    codebook_df = apply_additional_transformations(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)

    # Transform column names, sync and save the filtered codebook
    transformed_codebook = transform_column_names(filtered_codebook.copy(), is_codebook=True)
    transformed_codebook = sync_codebook_columns(transformed_codebook)
    save_filtered_codebook(transformed_codebook)

if __name__ == "__main__":
    main()
