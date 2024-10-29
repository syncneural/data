# utils.py

import re
import pandas as pd

def transform_column_names(df_latest, codebook_df):
    """
    Transforms the column names of a DataFrame based on a codebook DataFrame.
    This includes converting to title case, replacing underscores with spaces, and adding units where applicable.
    """
    rename_map = {}
    for _, row in codebook_df.iterrows():
        col_name, unit = row['column'], row['unit']
        if col_name in df_latest.columns:
            new_col_name = col_name.replace("_", " ").title()
            # Append unit to the column name as needed
            if isinstance(unit, str):
                normalized_unit = unit.lower().replace(" ", "").replace("co₂", "co2").strip()
                if "terawatt-hours" in normalized_unit:
                    new_col_name += " kWh"
                elif "kilowatt-hours" in normalized_unit:
                    new_col_name += " kWh"
                elif "gramsofco2equivalentsperkilowatt-hour" in normalized_unit:
                    new_col_name += " gCO₂e/kWh"
                elif "%" in normalized_unit:
                    new_col_name += " %"
                elif "international-$" in normalized_unit:
                    new_col_name += " ISD"
            rename_map[col_name] = new_col_name
    
    df_latest = df_latest.rename(columns=rename_map)
    essential_columns = {
        'country': 'Country',
        'iso code': 'ISO Code',
        'year': 'Year',
        'gdp isd': 'GDP ISD',
        'latest_data_year': 'Latest Data Year'
    }
    df_latest.columns = [essential_columns.get(col.lower(), col) for col in df_latest.columns]
    return df_latest
