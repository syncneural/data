import os
import logging
import yaml
import polars as pl
from utils import transform_column_names, apply_transformations

# Setup logger
logger = logging.getLogger("CodebookUpdater")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(name)s - %(message)s')
ch.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(ch)

def load_codebook(codebook_path='owid-energy-codebook.csv') -> pl.DataFrame:
    logger.info(f"Loading codebook from {codebook_path}")
    return pl.read_csv(codebook_path)

def load_or_create_config(codebook_df, config_path='config.yaml') -> dict:
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        columns_to_keep = codebook_df['column'].to_list()
        config = {'columns_to_keep': columns_to_keep, 'active_year': 2022, 'previousYearRange': 5, 'force_update': True}
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
            logger.info(f"Created default configuration at {config_path}")
    return config

def filter_codebook(codebook_df, config) -> pl.DataFrame:
    filtered_codebook = codebook_df.filter(pl.col("column").is_in(config['columns_to_keep']))
    logger.debug(f"Filtered codebook columns: {filtered_codebook.columns}")
    return filtered_codebook

import polars as pl

def sync_codebook_columns(filtered_codebook: pl.DataFrame, transformed_codebook: pl.DataFrame) -> pl.DataFrame:
    """
    Syncs the codebook to include all columns from the processed dataset, 
    preserving existing descriptions, units, and sources from the original codebook, 
    with transformations for column names and specific terms in descriptions.

    Args:
        filtered_codebook (pl.DataFrame): The original codebook with metadata.
        transformed_codebook (pl.DataFrame): The codebook with updated column names.

    Returns:
        pl.DataFrame: Final codebook with original metadata, updated descriptions, and column order matching processed data.
    """
    # Step 1: Load the processed dataset to align the final order
    processed_data = pl.read_csv("output/processed_energy_data.csv")
    transformed_columns = processed_data.columns

    # Step 2: Check for columns in processed_data but not in original codebook
    missing_columns = [col for col in transformed_columns if col not in filtered_codebook["column"].to_list()]

    # Step 3: Create empty entries for truly new columns
    missing_rows = pl.DataFrame({
        "column": missing_columns,
        "description": ["No description available"] * len(missing_columns),
        "unit": [None] * len(missing_columns),
        "source": ["Calculated"] * len(missing_columns)
    })

    # Step 4: Concatenate filtered_codebook with any missing rows
    combined_codebook = pl.concat([filtered_codebook, missing_rows], how="vertical")

    # Step 5: Apply transformations to column names and specific terms in descriptions
    combined_codebook = combined_codebook.with_columns([
        # Rename columns for consistency with processed data
        pl.when(pl.col("unit").str.contains("terawatt-hours", literal=True))
        .then(pl.col("unit").str.replace_all("terawatt-hours", "kilowatt-hours", literal=True))
        .otherwise(pl.col("unit"))
        .alias("unit"),
        
        # Add descriptive clarification for percentage columns
        pl.when(pl.col("unit").str.contains("%", literal=True))
        .then(pl.col("description").apply(lambda desc: f"{desc} (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)" if "percentage fraction of 1" not in desc else desc))
        .otherwise(pl.col("description"))
        .alias("description")
    ])

    # Ensure column order matches the transformed columns
    column_order = {col: i for i, col in enumerate(transformed_columns)}
    combined_codebook = combined_codebook.sort(
        by=pl.col("column").apply(lambda x: column_order.get(x, float("inf"))))

    return combined_codebook

def save_filtered_codebook(codebook_df, output_dir='output', filename='codebook.csv'):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    codebook_df.write_csv(output_path)
    logger.info(f"Filtered codebook saved to '{output_path}'")

def main():
    logger.info("Starting update_codebook script.")
    codebook_df = load_codebook()
    config = load_or_create_config(codebook_df)
    
    # Filtering and transformations
    filtered_codebook = filter_codebook(codebook_df, config)
    logger.debug(f"Filtered Codebook:\n{filtered_codebook}")
    transformed_codebook = transform_column_names(filtered_codebook, is_codebook=True)
    logger.debug(f"Transformed Codebook Columns:\n{transformed_codebook['column'].to_list()}")

    # Sync columns and log full DataFrame before saving
    final_codebook = sync_codebook_columns(filtered_codebook, transformed_codebook)
    logger.debug(f"Final Codebook Before Save:\n{final_codebook}")

    # Save the final codebook
    save_filtered_codebook(final_codebook)

if __name__ == "__main__":
    main()
