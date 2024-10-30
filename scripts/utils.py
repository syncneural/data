import logging
import re
import pandas as pd

logger = logging.getLogger("Utils")

def transform_column_names(df, is_codebook=False):
    """
    Transforms the column names in the DataFrame.
    If is_codebook is True, it operates on the 'column' column and uses the 'unit' column from the codebook DataFrame.
    If False, it operates on the DataFrame's columns and requires a mapping of units.
    """
    if is_codebook:
        columns = df['column'].tolist()
        units = df['unit'].tolist()
    else:
        columns = df.columns.tolist()
        units = getattr(df, 'units', [None] * len(columns))  # Assuming units are provided or None

    new_columns = []
    for idx, col_name in enumerate(columns):
        original_col_name = col_name  # Keep the original for logging
        # Transform column name to Title Case and replace underscores
        new_col_name = col_name.replace("_", " ").title()

        # Correct specific cases after title casing
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

        # Append unit to the column name if needed
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
            # Include other unit mappings as needed

        new_columns.append(new_col_name)
        logger.info(f"Transformed '{original_col_name}' to '{new_col_name}'")

    if is_codebook:
        df['column'] = new_columns
    else:
        df.columns = new_columns

    return df

def apply_transformations(codebook_df):
    # Replace 'terawatt-hours' with 'kilowatt-hours' in 'description' and 'unit' columns
    codebook_df['description'] = codebook_df['description'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)terawatt-hours', 'kilowatt-hours', regex=True)
    logger.info("Applied transformation: Replaced 'terawatt-hours' with 'kilowatt-hours' in description and unit columns.")

    # Similarly for 'million tonnes' to 'tonnes'
    codebook_df['description'] = codebook_df['description'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    codebook_df['unit'] = codebook_df['unit'].str.replace(
        r'(?i)million tonnes', 'tonnes', regex=True)
    logger.info("Applied transformation: Replaced 'million tonnes' with 'tonnes' in description and unit columns.")

    # Update descriptions for percentage columns
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

def apply_unit_conversion(df, codebook_df):
    """
    Applies unit conversions to the DataFrame based on units in the codebook.
    Also updates the codebook units accordingly.
    """
    for idx, row in codebook_df.iterrows():
        col = row['column']
        unit = row['unit']
        if col in df.columns and isinstance(unit, str):
            normalized_unit = unit.lower()
            if 'terawatt-hours' in normalized_unit:
                df[col] = df[col] * 1e9  # Convert TWh to kWh
                # Update the unit in the codebook
                codebook_df.at[idx, 'unit'] = normalized_unit.replace('terawatt-hours', 'kilowatt-hours')
                logger.info(f"Converted {col} from TWh to kWh in dataset and updated unit in codebook.")
            elif 'million tonnes' in normalized_unit:
                df[col] = df[col] * 1e6  # Convert million tonnes to tonnes
                codebook_df.at[idx, 'unit'] = normalized_unit.replace('million tonnes', 'tonnes')
                logger.info(f"Converted {col} from million tonnes to tonnes in dataset and updated unit in codebook.")
            # Add other unit conversions as needed
    return df, codebook_df
