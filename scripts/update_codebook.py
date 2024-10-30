import os
import logging
import pandas as pd
import yaml
from utils import transform_column_names

logger = logging.getLogger("CodebookUpdater")
logging.basicConfig(level=logging.INFO)

def load_codebook():
    # Load the original codebook
    codebook_df = pd.read_csv('owid-energy-codebook.csv')
    return codebook_df

def load_or_create_config(codebook_df):
    config_path = 'config.yaml'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        # Default configuration if config.yaml does not exist
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

def filter_codebook(codebook_df, config):
    # Keep only the rows where 'column' is in config['columns_to_keep']
    filtered_codebook = codebook_df[codebook_df['column'].isin(config['columns_to_keep'])].copy()
    return filtered_codebook

def apply_transformations(filtered_codebook):
    # Replace 'terawatt' with 'kilowatt' in 'description' and 'unit' columns
    filtered_codebook['description'] = filtered_codebook['description'].str.replace(
        r'(?i)terawatt', 'kilowatt', regex=True)
    filtered_codebook['unit'] = filtered_codebook['unit'].str.replace(
        r'(?i)terawatt', 'kilowatt', regex=True)
    logger.info("Applied transformation: Replaced 'terawatt' with 'kilowatt' in description and unit columns.")

    # Transform column names using utils.py
    filtered_codebook = transform_column_names(filtered_codebook, is_codebook=True)

    return filtered_codebook

def sync_codebook_columns(filtered_codebook):
    # Load processed data
    processed_data = pd.read_csv('output/processed_energy_data.csv')
    transformed_columns = processed_data.columns.tolist()

    # Update the 'column' names in the codebook to match the processed data
    if len(filtered_codebook) == len(transformed_columns):
        filtered_codebook['column'] = transformed_columns
    else:
        logger.warning("Mismatch in number of columns between codebook and processed data.")
        # Map existing columns
        column_mapping = dict(zip(filtered_codebook['column'], transformed_columns))
        filtered_codebook['column'] = filtered_codebook['column'].map(column_mapping)

    return filtered_codebook

def save_filtered_codebook(filtered_codebook):
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, 'codebook.csv')
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

def main():
    logger.info("Starting update_codebook script.")
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)
    transformed_codebook = apply_transformations(filtered_codebook)
    transformed_codebook = sync_codebook_columns(transformed_codebook)
    save_filtered_codebook(transformed_codebook)

if __name__ == "__main__":
    main()
