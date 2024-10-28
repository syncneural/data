# Step 1: Import required libraries
import pandas as pd
import logging
import yaml

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

# Step 4: Filter codebook dataset for relevant rows and output with all columns
def filter_codebook(codebook_df, config):
    rows_to_keep = config['columns_to_keep']
    filtered_codebook = codebook_df[codebook_df['column'].isin(rows_to_keep)]
    return filtered_codebook

# Step 5: Save the filtered codebook dataset
def save_filtered_codebook(filtered_codebook):
    output_path = 'output/codebook.csv'
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

# Step 6: Main function
def main():
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)
    save_filtered_codebook(filtered_codebook)

# Run main function
if __name__ == "__main__":
    main()
