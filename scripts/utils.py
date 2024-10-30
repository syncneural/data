import logging

logger = logging.getLogger("CodebookUpdater")

def transform_column_names(codebook_df):
    """
    Transforms the 'column' names in the codebook DataFrame.
    This includes converting to title case, replacing underscores with spaces, and appending units.
    """
    for idx, row in codebook_df.iterrows():
        col_name, unit = row['column'], row['unit']
        
        # Transform column name to Title Case and replace underscores
        new_col_name = col_name.replace("_", " ").title()
        
        # Append unit to the column name if needed
        if isinstance(unit, str) and unit.strip():  # Skip empty or NaN units
            normalized_unit = unit.lower().replace(" ", "").replace("co₂", "co2").strip()
            if "terawatt-hours" in normalized_unit or "kilowatt-hours" in normalized_unit:
                new_col_name += " kWh"
            elif "gramsofco2equivalentsperkilowatt-hour" in normalized_unit:
                new_col_name += " gCO₂e/kWh"
            elif "%" in normalized_unit:
                new_col_name += " %"
            elif "international-$" in normalized_unit:
                new_col_name += " ISD"
        
        # Apply the transformation to the row in codebook_df
        codebook_df.at[idx, 'column'] = new_col_name
        logger.info(f"Transformed '{col_name}' to '{new_col_name}'")

    # Log final columns
    logger.info(f"Final transformed 'column' values: {codebook_df['column'].tolist()}")
    return codebook_df
