# Step 1: Import required libraries
import pandas as pd
import logging
import yaml
import re
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CodebookUpdater")

# Step 2: Load configuration from config.yaml
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
    logger.debug("Attempting to load owid-energy-codebook.csv.")
    try:
        df = pd.read_csv('owid-energy-codebook.csv')
        logger.info("Codebook loaded successfully.")
        return df
    except FileNotFoundError:
        logger.error("owid-energy-codebook.csv not found.")
        raise

# Step 4: Load processed data to get column names
def load_processed_data():
    logger.debug("Attempting to load processed_energy_data.csv.")
    try:
        df = pd.read_csv('output/processed_energy_data.csv')
        logger.info("Processed data loaded successfully.")
        return df
    except FileNotFoundError:
        logger.error("processed_energy_data.csv not found in 'output' directory.")
        raise

# Step 5: Filter codebook dataset for relevant rows based on config
def filter_codebook(codebook_df, config):
    rows_to_keep = config['columns_to_keep']
    filtered_codebook = codebook_df[codebook_df['column'].isin(rows_to_keep)]
    logger.info("Filtered the codebook dataset based on configuration.")
    return filtered_codebook

# Step 6: Apply transformations to the filtered codebook dataset
def apply_transformations(filtered_codebook):
    logger.debug("Applying transformations to replace 'terawatt' with 'kilowatt' in the description and unit columns.")
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
    output_dir = 'output'
    output_path = os.path.join(output_dir, 'codebook.csv')
    
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        logger.debug(f"Output directory '{output_dir}' does not exist. Creating it.")
        os.makedirs(output_dir)
    else:
        logger.debug(f"Output directory '{output_dir}' already exists.")

    logger.debug(f"Attempting to save filtered codebook to {output_path}. DataFrame shape: {filtered_codebook.shape}")
    try:
        filtered_codebook.to_csv(output_path, index=False)
        logger.info(f"Filtered codebook saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save the filtered codebook: {e}")

# Step 9: Main function
def main():
    logger.info("Starting update_codebook script.")
    try:
        codebook_df = load_codebook()
        processed_data_df = load_processed_data()
        config = load_or_create_config(codebook_df)
        filtered_codebook = filter_codebook(codebook_df, config)
        transformed_codebook = apply_transformations(filtered_codebook)
        transformed_codebook = fix_column_names(transformed_codebook, codebook_df)
        save_filtered_codebook(transformed_codebook)
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")

# Run main function
if __name__ == "__main__":
    main()
