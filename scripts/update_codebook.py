# Step 1: Import required libraries
import pandas as pd
import logging
import yaml
import re

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

# Step 5: Match and update codebook with processed data column names
def update_codebook_with_processed_columns(filtered_codebook, processed_data_df):
    processed_columns = processed_data_df.columns.tolist()
    for col in processed_columns:
        if col in filtered_codebook['column'].values:
            # Update codebook entries where the column matches
            logger.info(f"Updating codebook for column: {col}")
            filtered_codebook.loc[filtered_codebook['column'] == col, 'column'] = col
        else:
            logger.warning(f"Column from processed data not found in codebook: {col}")
    return filtered_codebook

# Step 6: Filter codebook dataset for relevant rows and output with all columns
def filter_codebook(codebook_df, config):
    rows_to_keep = config['columns_to_keep']
    filtered_codebook = codebook_df[codebook_df['column'].isin(rows_to_keep)]
    return filtered_codebook

# Step 7: Apply transformations to the filtered codebook dataset
def apply_transformations(filtered_codebook):
    # Replace all instances of 'terawatt' with 'kilowatt' in both 'description' and 'unit' columns
    filtered_codebook['description'] = filtered_codebook['description'].apply(lambda x: re.sub(r'(?i)terawatt', 'kilowatt', x) if pd.notna(x) else x)
    filtered_codebook['unit'] = filtered_codebook['unit'].apply(lambda x: re.sub(r'(?i)terawatt', 'kilowatt', x) if pd.notna(x) else x)
    logger.info("Applied transformation: Replaced 'terawatt' with 'kilowatt' in description and unit columns.")
    return filtered_codebook

# Step 8: Fix column names using utils.py
from utils import transform_column_names

def fix_column_names(filtered_codebook, codebook_df):
    filtered_codebook = transform_column_names(filtered_codebook, codebook_df)
    logger.info("Fixed column names using the utility function from utils.py.")
    return filtered_codebook

# Step 9: Save the filtered, transformed, and updated codebook dataset
def save_filtered_codebook(filtered_codebook):
    output_path = 'output/codebook.csv'
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

# Step 10: Main function
def main():
    codebook_df = load_codebook()
    processed_data_df = load_processed_data()
    config = load_or_create_config(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)
    updated_codebook = update_codebook_with_processed_columns(filtered_codebook, processed_data_df)
    transformed_codebook = apply_transformations(updated_codebook)
    transformed_codebook = fix_column_names(transformed_codebook, codebook_df)
    save_filtered_codebook(transformed_codebook)

# Run main function
if __name__ == "__main__":
    main()
