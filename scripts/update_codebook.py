import os
import logging
import pandas as pd
import yaml
from utils import transform_column_names

logger = logging.getLogger("CodebookUpdater")
logging.basicConfig(level=logging.INFO)

def load_codebook():
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
    # Replace 'terawatt-hours' with 'kilowatt-hours' in 'description' and 'unit' columns
    filtered_codebook['description'] = filtered_codebook['description'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    filtered_codebook['unit'] = filtered_codebook['unit'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    logger.info("Applied transformation: Replaced 'terawatt-hours' with 'kilowatt-hours' in description and unit columns.")

    # Similarly for 'million tonnes' to 'tonnes'
    filtered_codebook['description'] = filtered_codebook['description'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    filtered_codebook['unit'] = filtered_codebook['unit'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    logger.info("Applied transformation: Replaced 'million tonnes' with 'tonnes' in description and unit columns.")

    # Update descriptions for percentage columns
    percentage_columns = filtered_codebook[filtered_codebook['unit'].str.contains('%', na=False)]['column'].tolist()
    for col in percentage_columns:
        idx = filtered_codebook[filtered_codebook['column'] == col].index[0]
        original_description = filtered_codebook.at[idx, 'description']
        if "(Measured as a percentage fraction of 1" not in original_description:
            filtered_codebook.at[idx, 'description'] = original_description + " (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)"
            logger.info(f"Updated description for {col} to indicate percentage fraction.")

    # Transform column names using utils.py
    filtered_codebook = transform_column_names(filtered_codebook, is_codebook=True)

    return filtered_codebook

def sync_codebook_columns(filtered_codebook):
    # Load processed data
    processed_data = pd.read_csv('output/processed_energy_data.csv')
    transformed_columns = processed_data.columns.tolist()
    
    # Update the 'column' names in the codebook to match the processed data
    codebook_columns = filtered_codebook['column'].tolist()
    new_columns = [col for col in transformed_columns if col not in codebook_columns]
    
    # Add new columns to the codebook
    for col in new_columns:
        # Add new rows for the new columns
        filtered_codebook = pd.concat([filtered_codebook, pd.DataFrame({
            'column': [col],
            'description': ['Derived metric'],
            'unit': [''],  # Specify unit if known
            'source': ['Calculated']
        })], ignore_index=True)
    
    # **Reorder the codebook to match the order of transformed_columns**
    filtered_codebook = filtered_codebook.set_index('column').reindex(transformed_columns).reset_index()
    
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
