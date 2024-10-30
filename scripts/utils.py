import logging
import re
import pandas as pd

logger = logging.getLogger("ColumnTransformer")

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
        units = df.units if hasattr(df, 'units') else [None] * len(columns)  # Assuming units are provided or None

    new_columns = []
    for idx, col_name in enumerate(columns):
        # Transform column name to Title Case and replace underscores
        new_col_name = col_name.replace("_", " ").title()

        # Correct specific cases after title casing
        new_col_name = new_col_name.replace('Iso Code', 'ISO Code')
        new_col_name = new_col_name.replace('Gdp', 'GDP')
        new_col_name = new_col_name.replace('Co2', 'CO₂').replace('Co2e', 'CO₂e')

        # Append unit to the column name if needed
        unit = units[idx] if units else None
        if pd.notna(unit) and str(unit).strip():
            normalized_unit = re.sub(r'\s+', '', str(unit).lower()).strip()
            normalized_unit = normalized_unit.replace('co₂', 'co2')

            if 'kilowatt-hours' in normalized_unit:
                new_col_name += ' kWh'
            elif 'gramsofco2equivalentsperkilowatt-hour' in normalized_unit:
                new_col_name += ' gCO₂e/kWh'
            elif '%' in normalized_unit:
                new_col_name += ' %'
            elif 'international-$' in normalized_unit:
                new_col_name += ' ISD'
            # Include other unit mappings as needed

        new_columns.append(new_col_name)
        logger.info(f"Transformed '{col_name}' to '{new_col_name}'")

    if is_codebook:
        df['column'] = new_columns
    else:
        df.columns = new_columns

    return df
