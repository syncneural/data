# Step 1: Import required libraries
import pandas as pd
import logging
import yaml
import re
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CodebookUpdater")

# Step 2: Load configuration from config.yaml
# If the config file does not exist, create one from the existing columns

def load_or_create_config(codebook_df):
    config_path = 'config.yaml'
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.info(f"Config file not found. Creating new config.yaml with current columns.")
        rows_to_keep = codebook_df['column'].unique().tolist()
        config = {'columns_to_keep': rows_to_keep}
        with open(config_path, 'w') as file:
            yaml.dump(config, file)
    return config

# Step 3: Load the codebook dataset
def load_codebook():
    return pd.read_csv('owid-energy-codebook.csv')

# Step 4: Load processed data to get column names
def load_processed_data():
    return pd.read_csv('output/processed_energy_data.csv')

# Step 5: Filter codebook dataset for relevant rows based on config
def filter_codebook(codebook_df, config):
    rows_to_keep = config['columns_to_keep']
    filtered_codebook = codebook_df[codebook_df['column'].isin(rows_to_keep)]
    return filtered_codebook

# Step 6: Apply transformations to the filtered codebook dataset
def apply_transformations(filtered_codebook):
    # Replace all instances of 'terawatt' with 'kilowatt' in both 'description' and 'unit' columns
    filtered_codebook['description'] = filtered_codebook['description'].apply(lambda x: re.sub(r'(?i)terawatt', 'kilowatt', x) if pd.notna(x) else x)
    filtered_codebook['unit'] = filtered_codebook['unit'].apply(lambda x: re.sub(r'(?i)terawatt', 'kilowatt', x) if pd.notna(x) else x)
    logger.info("Applied transformation: Replaced 'terawatt' with 'kilowatt' in description and unit columns.")
    return filtered_codebook

# Step 7: Fix column names using utils.py
from utils import transform_column_names

def fix_column_names(filtered_codebook, codebook_df):
    filtered_codebook = transform_column_names(filtered_codebook, codebook_df)
    logger.info("Fixed column names using the utility function from utils.py.")
    return filtered_codebook

# Step 8: Save the filtered, transformed, and updated codebook dataset
def save_filtered_codebook(filtered_codebook):
    output_path = 'output/codebook.csv'
    # Ensure the output directory exists
    if not os.path.exists('output'):
        os.makedirs('output')
    logger.debug(f"Attempting to save the file to {output_path}. DataFrame shape: {filtered_codebook.shape}")
    try:
        filtered_codebook.to_csv(output_path, index=False)
        logger.info(f"Filtered codebook saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save the filtered codebook: {e}")

# Step 9: Main function
def main():
    codebook_df = load_codebook()
    processed_data_df = load_processed_data()
    config = load_or_create_config(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)
    transformed_codebook = apply_transformations(filtered_codebook)
    transformed_codebook = fix_column_names(transformed_codebook, codebook_df)
    save_filtered_codebook(transformed_codebook)

# Run main function
if __name__ == "__main__":
    main()
