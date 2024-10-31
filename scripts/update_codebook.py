# scripts/update_codebook.py

import os
import logging
import yaml
import polars as pl
from utils import (
    transform_column_names,
    apply_transformations
)

logger = logging.getLogger("CodebookUpdater")
logger.setLevel(logging.INFO)  # Set to INFO or DEBUG as needed

# Create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)  # Set to DEBUG for detailed logs

# Create formatter and add it to the handlers
formatter = logging.Formatter('[%(levelname)s] %(name)s - %(message)s')
ch.setFormatter(formatter)

# Add the handlers to the logger
if not logger.hasHandlers():
    logger.addHandler(ch)

def load_codebook(codebook_path: str = 'owid-energy-codebook.csv') -> pl.DataFrame:
    """
    Loads the codebook from a CSV file.
    
    Args:
        codebook_path (str): Path to the codebook CSV.
        
    Returns:
        pl.DataFrame: The codebook DataFrame.
    """
    return pl.read_csv(codebook_path)

def load_or_create_config(codebook_df: pl.DataFrame, config_path: str = 'config.yaml') -> dict:
    """
    Loads the configuration from a YAML file or creates a default config.
    
    Args:
        codebook_df (pl.DataFrame): The codebook DataFrame.
        config_path (str): Path to the config YAML file.
        
    Returns:
        dict: Configuration dictionary.
    """
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
    """
    Filters the codebook based on the configuration.
    
    Args:
        codebook_df (pl.DataFrame): The codebook DataFrame.
        config (dict): Configuration dictionary.
        
    Returns:
        pl.DataFrame: Filtered codebook DataFrame.
    """
    return codebook_df.filter(pl.col("column").is_in(config['columns_to_keep']))

def sync_codebook_columns(filtered_codebook: pl.DataFrame, processed_data_path: str = 'output/processed_energy_data.csv') -> pl.DataFrame:
    """
    Synchronizes the codebook with the processed data by adding new derived metrics.
    
    Args:
        filtered_codebook (pl.DataFrame): The filtered codebook DataFrame.
        processed_data_path (str): Path to the processed energy data CSV.
        
    Returns:
        pl.DataFrame: Synchronized codebook DataFrame.
    """
    processed_data = pl.read_csv(processed_data_path)
    transformed_columns = processed_data.columns
    
    codebook_columns = filtered_codebook["column"].to_list()
    new_columns = [col for col in transformed_columns if col not in codebook_columns]
    
    if new_columns:
        new_entries = pl.DataFrame({
            'column': new_columns,
            'description': ['Derived metric'] * len(new_columns),
            'unit': [''] * len(new_columns),
            'source': ['Calculated'] * len(new_columns)
        })
        filtered_codebook = filtered_codebook.vstack(new_entries)
        logger.debug(f"Added derived metrics: {new_columns}")
    
    # Reorder to match processed data columns
    filtered_codebook = filtered_codebook.set_index("column").select(transformed_columns).fill_null("description", "No description available").fill_null("unit", "")
    
    return filtered_codebook

def save_filtered_codebook(filtered_codebook: pl.DataFrame, output_path: str = 'output/codebook.csv') -> None:
    """
    Saves the filtered codebook to a CSV file.
    
    Args:
        filtered_codebook (pl.DataFrame): The filtered codebook DataFrame.
        output_path (str): Path to save the filtered codebook CSV.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    filtered_codebook.write_csv(output_path)
    logger.info(f"Filtered codebook saved to '{output_path}'")

def main():
    logger.info("Starting update_codebook script.")
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)
    filtered_codebook = filter_codebook(codebook_df, config)
    codebook_transformed = apply_transformations(filtered_codebook)
    codebook_synchronized = sync_codebook_columns(codebook_transformed)
    save_filtered_codebook(codebook_synchronized)

if __name__ == "__main__":
    main()
