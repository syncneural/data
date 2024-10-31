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
    # Step 1: Load columns from processed dataset to align the final codebook
    processed_data = pl.read_csv("output/processed_energy_data.csv")
    transformed_columns = processed_data.columns  # Verified as Polars-compatible

    # Step 2: Create column order dictionary for correct sorting
    column_order = {col: i for i, col in enumerate(transformed_columns)}  # Verified as compatible

    # Step 3: Identify missing columns in the codebook and prepare placeholder entries
    missing_columns = [col for col in transformed_columns if col not in filtered_codebook["column"].to_list()]

    # Add rows for missing columns with placeholder descriptions
    missing_rows = pl.DataFrame({
        "column": missing_columns,
        "description": ["No description available"] * len(missing_columns),
        "unit": [None] * len(missing_columns),
        "source": ["Calculated"] * len(missing_columns)
    })

    # Combine the filtered_codebook with new rows for missing columns
    combined_codebook = pl.concat([filtered_codebook, missing_rows], how="vertical")

    # Step 4: Apply transformations
    combined_codebook = combined_codebook.with_columns([
        # Convert 'terawatt-hours' in the unit to 'kilowatt-hours'
        pl.when(pl.col("unit").str.contains("terawatt-hours", literal=True))
        .then(pl.lit("kilowatt-hours"))
        .otherwise(pl.col("unit"))
        .alias("unit"),

        # Add "percentage fraction of 1" clarification to percentage columns
        pl.when(pl.col("unit").str.contains("%", literal=True))
        .then(pl.col("description") + " (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)")
        .otherwise(pl.col("description"))
        .alias("description")
    ])

    # Step 5: Add a temporary 'sort_order' column based on `column_order`
    # Use `apply` on a Polars Series to generate 'sort_order'
    sort_order_series = combined_codebook["column"].apply(lambda col: column_order.get(col, float("inf")))
    combined_codebook = combined_codebook.with_columns(pl.Series("sort_order", sort_order_series))

    # Step 6: Sort by `sort_order` column and remove it afterward
    final_codebook = combined_codebook.sort("sort_order").drop("sort_order")

    return final_codebook

    
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
