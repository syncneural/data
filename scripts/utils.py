# scripts/utils.py

import logging
import re
import polars as pl

logger = logging.getLogger("Utils")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(name)s - %(message)s')
ch.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(ch)

def transform_column_names(df: pl.DataFrame, is_codebook: bool = False) -> pl.DataFrame:
    """
    Transforms column names based on predefined replacements and unit information.
    
    Args:
        df (pl.DataFrame): The DataFrame with original or codebook column names.
        is_codebook (bool): Flag indicating if the DataFrame is a codebook.
        
    Returns:
        pl.DataFrame: DataFrame with transformed column names.
    """
    if is_codebook:
        columns = df["column"].to_list()
        units = df["unit"].to_list()
    else:
        columns = df.columns
        units = [None] * len(columns)

    new_columns = []
    for idx, col_name in enumerate(columns):
        original_col_name = col_name
        new_col_name = col_name.replace("_", " ").title()

        replacements = {
            'Iso Code': 'ISO Code',
            'Gdp': 'GDP',
            'Co2': 'CO₂',
            'Co2e': 'CO₂e',
            'Gco₂e': 'gCO₂e',
            'Kwh': 'kWh',
            'Latest Data Year': 'Latest Data Year'
        }
        for old, new in replacements.items():
            new_col_name = new_col_name.replace(old, new)

        unit = units[idx] if units else None
        if isinstance(unit, str) and unit.strip():
            normalized_unit = re.sub(r'\s+', '', unit.lower()).strip()
            normalized_unit = normalized_unit.replace('co₂', 'co2')

            if 'kilowatt-hours' in normalized_unit:
                new_col_name += ' kWh'
            elif 'gco2e/kwh' in normalized_unit or 'gramsofco2equivalentsperkilowatt-hour' in normalized_unit:
                new_col_name += ' gCO₂e/kWh'
            elif '%' in normalized_unit:
                new_col_name += ' %'
            elif 'international-$' in normalized_unit:
                new_col_name += ' ISD'
            elif 'tonnes' in normalized_unit:
                new_col_name += ' tonnes'
            elif 'year' in normalized_unit:
                pass

        new_columns.append(new_col_name)
        logger.debug(f"Transformed '{original_col_name}' to '{new_col_name}'")

    if is_codebook:
        # Handle transformation of the 'column' field by creating a new DataFrame
        df_transformed = df.drop(["column"]).with_columns([
            pl.Series("column", new_columns)
        ])
    else:
        rename_map = {old: new for old, new in zip(columns, new_columns)}
        df_transformed = df.rename(rename_map)

    logger.debug(f"Final column names: {df_transformed.columns}")
    return df_transformed

def apply_unit_conversion(df: pl.DataFrame, codebook_df: pl.DataFrame) -> pl.DataFrame:
    """
    Applies unit conversions to the DataFrame based on the codebook's unit definitions.
    
    Args:
        df (pl.DataFrame): The DataFrame to convert units for.
        codebook_df (pl.DataFrame): The codebook containing unit definitions.
        
    Returns:
        pl.DataFrame: DataFrame with units converted.
    """
    df_converted = df.clone()
    for row in codebook_df.iter_rows(named=True):
        col = row['column']
        unit = row['unit']
        if col in df_converted.columns and isinstance(unit, str):
            normalized_unit = unit.lower()
            if 'terawatt-hour' in normalized_unit or 'twh' in normalized_unit:
                df_converted = df_converted.with_columns([(pl.col(col) * 1e9).alias(col)])
                logger.debug(f"Converted '{col}' from TWh to kWh")
    return df_converted


def update_codebook_units(codebook_df: pl.DataFrame) -> pl.DataFrame:
    """
    Updates the 'unit' field in the codebook DataFrame from 'terawatt-hours' or 'TWh' to 'kilowatt-hours'.

    Args:
        codebook_df (pl.DataFrame): The codebook DataFrame.

    Returns:
        pl.DataFrame: Updated codebook DataFrame with converted units.
    """
    mask_twh = codebook_df["unit"].str.to_lowercase().str.contains("terawatt-hour|twh")
    
    codebook_updated = codebook_df.with_columns(
        pl.when(mask_twh)
        .then(pl.lit("kilowatt-hours"))
        .otherwise(pl.col("unit"))
        .alias("unit")
    )
    
    if mask_twh.any():
        logger.debug("Updated units from 'terawatt-hours' to 'kilowatt-hours'")
    
    return codebook_updated


def apply_transformations(codebook_df: pl.DataFrame) -> pl.DataFrame:
    """
    Apply predefined transformations to the codebook DataFrame.

    Args:
        codebook_df (pl.DataFrame): The original codebook DataFrame.

    Returns:
        pl.DataFrame: Transformed codebook DataFrame.
    """
    # Transform 'terawatt-hours' to 'kilowatt-hours' in description and unit columns
    codebook_transformed = codebook_df.with_columns([
        pl.when(pl.col("description").str.contains("terawatt-hours", literal=True))
        .then(pl.col("description").str.replace_all("terawatt-hours", "kilowatt-hours", literal=True))
        .otherwise(pl.col("description"))
        .alias("description"),
        pl.when(pl.col("unit").str.contains("terawatt-hours", literal=True))
        .then(pl.col("unit").str.replace_all("terawatt-hours", "kilowatt-hours", literal=True))
        .otherwise(pl.col("unit"))
        .alias("unit")
    ])
    logger.debug("Replaced 'terawatt-hours' with 'kilowatt-hours' in description and unit columns.")

    # Update descriptions for percentage units
    percentage_columns = codebook_transformed.filter(pl.col("unit").str.contains("%", literal=True)).select("column").to_series().to_list()
    updated_descriptions = []
    for desc, col in zip(codebook_transformed["description"].to_list(), codebook_transformed["column"].to_list()):
        if col in percentage_columns and "Measured as a percentage fraction of 1" not in desc:
            updated_desc = re.sub(r'Measured as a percentage', '', desc, flags=re.IGNORECASE).strip()
            updated_desc += " (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)"
            updated_descriptions.append(updated_desc)
            logger.debug(f"Updated description for '{col}' to indicate percentage fraction.")
        else:
            updated_descriptions.append(desc)
    codebook_transformed = codebook_transformed.with_columns(
        pl.Series("description", updated_descriptions)
    )

    return codebook_transformed
