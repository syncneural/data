# scripts/update_codebook.py

import os
import logging
import polars as pl
import yaml
from utils import transform_column_names, apply_transformations

logger = logging.getLogger("CodebookUpdater")
logging.basicConfig(level=logging.INFO)

def load_codebook(codebook_path: str = 'owid-energy-codebook.csv') -> pl.DataFrame:
    return pl.read_csv(codebook_path)

def load_or_create_config(codebook_df: pl.DataFrame, config_path: str = 'config.yaml') -> dict:
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        columns_to_keep = codebook_df["column"].to_list()
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

def filter_codebook(codebook_df: pl.DataFrame, config: dict) -> pl.DataFrame:
    return codebook_df.filter(pl.col("column").is_in(config["columns_to_keep"]))

def sync_codebook_columns(filtered_codebook: pl.DataFrame) -> pl.DataFrame:
    processed_data = pl.read_csv("output/processed_energy_data.csv")
    transformed_columns = processed_data.columns

    filtered_codebook = filtered_codebook.filter(pl.col("column").is_in(transformed_columns))
    for col in transformed_columns:
        if col not in filtered_codebook["column"].to_list():
            new_row = pl.DataFrame({"column": [col], "description": ["Derived metric"], "unit": [""], "source": ["Calculated"]})
            filtered_codebook = pl.concat([filtered_codebook, new_row])
    return filtered_codebook

def save_filtered_codebook(filtered_codebook: pl.DataFrame, output_dir: str = 'output') -> None:
    os.makedirs(output_dir, exist_ok=True)
    filtered_codebook.write_csv(os.path.join(output_dir, 'codebook.csv'))
    logger.info(f"Filtered codebook saved to '{output_dir}/codebook.csv'")

def main():
    logger.info("Starting update_codebook script.")
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)
    codebook_df = apply_transformations(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)
    transformed_codebook = transform_column_names(filtered_codebook, is_codebook=True)
    transformed_codebook = sync_codebook_columns(transformed_codebook)
    save_filtered_codebook(transformed_codebook)

if __name__ == "__main__":
    main()
