# utils.py

import logging
import re
import pandas as pd

logger = logging.getLogger("Utils")

def transform_column_names(df, is_codebook=False):
    if is_codebook:
        columns = df['column'].tolist()
        units = df['unit'].tolist()
    else:
        columns = df.columns.tolist()
        units = [None] * len(columns)  # No units in df

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
        if pd.notna(unit) and str(unit).strip():
            normalized_unit = re.sub(r'\s+', '', str(unit).lower()).strip()
            normalized_unit = normalized_unit.replace('co₂', 'co2')

            if 'kilowatt-hours' in normalized_unit:
                new_col_name += ' kWh'
            elif 'gco2e/kwh' in normalized_unit or 'grams of co2 equivalents per kilowatt-hour' in normalized_unit:
                new_col_name += ' gCO₂e/kWh'
            elif '%' in normalized_unit:
                new_col_name += ' %'  # Keep '%' unit
            elif 'international-$' in normalized_unit:
                new_col_name += ' ISD'
            elif 'tonnes' in normalized_unit:
                new_col_name += ' tonnes'
            elif 'year' in normalized_unit:
                pass  # Do not append unit to year columns

        new_columns.append(new_col_name)
        logger.info(f"Transformed '{original_col_name}' to '{new_col_name}'")

    if is_codebook:
        df['column'] = new_columns
    else:
        df.columns = new_columns

    return df

def apply_transformations(codebook_df):
    codebook_df['description'] = codebook_df['description'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    logger.info("Applied transformation: Replaced 'terawatt-hours' with 'kilowatt-hours' in description and unit columns.")

    codebook_df['description'] = codebook_df['description'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    logger.info("Applied transformation: Replaced 'million tonnes' with 'tonnes' in description and unit columns.")

    percentage_columns = codebook_df[codebook_df['unit'].str.contains('%', na=False)]['column'].tolist()
    for col in percentage_columns:
        idx = codebook_df[codebook_df['column'] == col].index[0]
        original_description = codebook_df.at[idx, 'description']
        if "Measured as a percentage fraction of 1" not in original_description:
            updated_description = re.sub(r'Measured as a percentage', '', original_description, flags=re.IGNORECASE).strip()
            updated_description += " (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)"
            codebook_df.at[idx, 'description'] = updated_description
            logger.info(f"Updated description for {col} to indicate percentage fraction.")
    return codebook_df
