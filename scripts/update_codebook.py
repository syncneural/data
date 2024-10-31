# update_codebook.py

import os
import logging
import pandas as pd
import yaml
from utils import transform_column_names, apply_transformations

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
    filtered_codebook = codebook_df[codebook_df['column'].isin(config['columns_to_keep'])].copy()
    return filtered_codebook

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

def save_filtered_codebook(filtered_codebook):
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'codebook.csv')
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

def main():
    logger.info("Starting update_codebook script.")
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)

    codebook_df = apply_transformations(codebook_df)

    filtered_codebook = filter_codebook(codebook_df, config)
    transformed_codebook = transform_column_names(filtered_codebook.copy(), is_codebook=True)
    transformed_codebook = sync_codebook_columns(transformed_codebook)
    save_filtered_codebook(transformed_codebook)

if __name__ == "__main__":
    main()
