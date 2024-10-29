# utils.py

import re

def transform_column_names(column):
    """
    Transforms the given column name to match the standard format.
    This includes converting to title case, replacing underscores with spaces, and adding units where applicable.
    """
    # Convert underscores to spaces and capitalize each word
    column = column.replace("_", " ").title()

    # Append units as needed, based on specific rules
    normalized_unit = column.lower().replace(" ", "").replace("co2", "co₂").strip()
    
    if "terawatt-hours" in normalized_unit:
        column += " kWh"
    elif "kilowatt-hours" in normalized_unit:
        column += " kWh"
    elif "gramsofco2equivalentsperkilowatt-hour" in normalized_unit:
        column += " gCO₂e/kWh"
    elif "%" in normalized_unit:
        column += " %"
    elif "international-$" in normalized_unit:
        column += " ISD"
    
    return column
