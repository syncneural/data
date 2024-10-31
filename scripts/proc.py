# proc.py

import os
import logging
import pandas as pd
import yaml
import re
import requests
import threading

# Setting up logging
logger = logging.getLogger("CombinedScript")
logging.basicConfig(level=logging.INFO)

# Utility Functions

# Download datasets using threading
def download_datasets():
    if not os.path.exists('owid-energy-data.csv'):
        data_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'
        df = pd.read_csv(data_url)
        df.to_csv('owid-energy-data.csv', index=False)
        logger.info("Downloaded owid-energy-data.csv successfully.")
    else:
        logger.info("owid-energy-data.csv already exists.")

    if not os.path.exists('owid-energy-codebook.csv'):
        codebook_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-codebook.csv'
        codebook_df = pd.read_csv(codebook_url)
        codebook_df.to_csv('owid-energy-codebook.csv', index=False)
        logger.info("Downloaded owid-energy-codebook.csv successfully.")
    else:
        logger.info("owid-energy-codebook.csv already exists.")

# Load or create configuration function
def load_or_create_config():
    config_path = 'config.yaml'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        config = {
            'columns_to_keep': ['country', 'year', 'iso_code', 'population', 'gdp'],
            'timeline_columns_to_keep': ['country', 'iso_code', 'year', 'population', 'gdp', 'biofuel_electricity', 'carbon_intensity_elec', 'coal_electricity', 'electricity_demand', 'electricity_generation', 'fossil_electricity', 'gas_electricity', 'hydro_electricity', 'low_carbon_electricity', 'nuclear_electricity', 'oil_electricity', 'other_renewable_electricity', 'other_renewable_exc_biofuel_electricity', 'per_capita_electricity', 'renewables_electricity', 'solar_electricity', 'wind_electricity'],
            'active_year': 2022,
            'previousYearRange': 5,
            'timeline_start_year': 2010,
            'timeline_end_year': 2022,
            'force_update': True
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
            logger.info(f"Created default configuration at {config_path}")
    return config

# Load main dataset function
def load_main_dataset():
    df = pd.read_csv('owid-energy-data.csv')
    return df

# Load codebook function
def load_codebook():
    codebook_df = pd.read_csv('owid-energy-codebook.csv')
    return codebook_df

# Filter dataset by year range
def filter_dataset_by_year_range(df, start_year, end_year):
    return df[(df['year'] >= start_year) & (df['year'] <= end_year)]

# Filter dataset columns
def filter_dataset_columns(df, columns_to_keep):
    return df[columns_to_keep].copy()

# Sync codebook columns with processed energy data
def sync_codebook_columns(filtered_codebook):
    processed_data = pd.read_csv('output/processed_energy_data.csv')
    transformed_columns = processed_data.columns.tolist()
    codebook_columns = filtered_codebook['column'].tolist()
    new_columns = [col for col in transformed_columns if col not in codebook_columns]

    for col in new_columns:
        filtered_codebook = pd.concat([filtered_codebook, pd.DataFrame({
            'column': [col],
            'description': ['Derived metric'],
            'unit': [''],
            'source': ['Calculated']
        })], ignore_index=True)

    filtered_codebook = filtered_codebook.set_index('column').reindex(transformed_columns).reset_index()

    filtered_codebook['description'] = filtered_codebook['description'].fillna('No description available')
    filtered_codebook['unit'] = filtered_codebook['unit'].fillna('')

    return filtered_codebook

# Save filtered codebook to output directory
def save_filtered_codebook(filtered_codebook):
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'codebook.csv')
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

# Apply transformations to codebook
def apply_transformations(codebook_df):
    """
    Apply transformations to codebook DataFrame.
    This function is only used in update_codebook.py.
    """
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
            logger.info(f"Updated description for '{col}' to indicate percentage fraction.")
    return codebook_df

# Core Processing Functions

# Filter main dataset function
def filter_main_dataset(df, config):
    df_filtered = df[config['columns_to_keep']].copy()
    df_filtered.dropna(subset=['population'], inplace=True)
    return df_filtered

# Filter year range function
def filter_year_range(df, config):
    active_year = config['active_year']
    previous_year_range = config['previousYearRange']
    start_year = active_year - previous_year_range
    df_filtered = df[df['year'] >= start_year]
    return df_filtered

# Prioritize active year function
def prioritize_active_year(df, config):
    df = df.sort_values(['country', 'iso_code', 'year'], ascending=[True, True, False])
    df_latest = df.groupby(['country', 'iso_code']).first().reset_index()
    df_latest['latest_data_year'] = df_latest['year']
    return df_latest

# Fetch GDP data range from World Bank API
def fetch_gdp_data_range(country_code, start_year, end_year, result_dict):
    year_range = f"{start_year}:{end_year}"
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?date={year_range}&format=json"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            gdp_data = {}
            if len(data) > 1 and data[1]:
                for item in data[1]:
                    gdp_value = item['value']
                    year = int(item['date'])
                    if gdp_value is not None:
                        gdp_data[year] = gdp_value
                result_dict[country_code] = gdp_data
            else:
                logger.error(f"No GDP data found for {country_code}")
                result_dict[country_code] = None
        else:
            logger.error(f"Failed to fetch GDP data for {country_code} from World Bank API, status code: {response.status_code}")
            result_dict[country_code] = None
    except Exception as e:
        logger.error(f"Exception occurred while fetching GDP data for {country_code}: {e}")
        result_dict[country_code] = None

# Fill missing GDP using World Bank API
def fill_gdp_using_world_bank(df, active_year, previousYearRange):
    missing_gdp = df[df['gdp'].isna() & df['iso_code'].notna()]
    threads = []
    gdp_results = {}

    for index, row in missing_gdp.iterrows():
        country_code = row['iso_code']
        thread = threading.Thread(target=fetch_gdp_data_range, args=(country_code, active_year - previousYearRange, active_year, gdp_results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    for index, row in missing_gdp.iterrows():
        country_code = row['iso_code']
        gdp_data = gdp_results.get(country_code, None)
        if gdp_data:
            latest_year = max(gdp_data.keys())
            df.loc[index, 'gdp'] = gdp_data[latest_year]
            df.loc[index, 'latest_data_year'] = latest_year
            logger.info(f"Filled GDP for {row['country']} using year: {latest_year}")
        else:
            logger.warning(f"No GDP data available within the specified range for {row['country']}")
    return df

# Apply unit conversion to dataset
def apply_unit_conversion_script(df, codebook_df):
    def apply_unit_conversion(df, codebook_df):
        for idx, row in codebook_df.iterrows():
            col = row['column']
            unit = row['unit']
            if col in df.columns and isinstance(unit, str):
                normalized_unit = unit.lower()
                if 'terawatt-hour' in normalized_unit or 'twh' in normalized_unit:
                    df[col] = df[col] * 1e9  # Convert TWh to kWh
                    logger.info(f"Converted '{col}' from TWh to kWh")
                elif 'million tonne' in normalized_unit or 'million tonnes' in normalized_unit:
                    df[col] = df[col] * 1e6  # Convert million tonnes to tonnes
                    logger.info(f"Converted '{col}' from million tonnes to tonnes")
                else:
                    logger.info(f"No conversion needed for '{col}' with unit '{unit}'")
        return df
    df = apply_unit_conversion(df, codebook_df)
    return df

# Update codebook units after conversion
def update_codebook_units_after_conversion(codebook_df):
    for idx, row in codebook_df.iterrows():
        unit = row['unit']
        if isinstance(unit, str):
            normalized_unit = unit.lower()
            if 'terawatt-hour' in normalized_unit or 'twh' in normalized_unit:
                codebook_df.at[idx, 'unit'] = 'kilowatt-hours'
                logger.info(f"Updated unit for '{row['column']}' to 'kilowatt-hours'")
    return codebook_df

# Round numeric columns
def round_numeric_columns(df):
    columns_to_round_0 = ['Population', 'GDP ISD']

    kwh_columns = [col for col in df.columns if 'kwh' in col.lower()]
    carbon_intensity_columns = [col for col in df.columns if 'gco₂e/kwh' in col.lower() or 'gco2e/kwh' in col.lower()]

    columns_to_round_0.extend(kwh_columns)
    columns_to_round_0.extend(carbon_intensity_columns)
    columns_to_round_0 = list(set(columns_to_round_0))

    for col in columns_to_round_0:
        if col in df.columns:
            df[col] = df[col].round(0).astype('Int64')
            logger.info(f"Rounded '{col}' to zero decimal places.")

    percentage_columns = [col for col in df.columns if '%' in col.lower()]

    for col in percentage_columns:
        if col in df.columns:
            df[col] = df[col] / 100.0  # Convert to fraction of 1
            df[col] = df[col].round(2)  # Round to two decimal places
            logger.info(f"Converted percentage column '{col}' to fraction of 1 and rounded to 2 decimal places.")

    return df

# Rename columns based on codebook
def rename_columns(df_latest, codebook_df):
    if 'latest_data_year' not in codebook_df['column'].values:
        new_row = pd.DataFrame({
            'column': ['latest_data_year'],
            'description': ['Year of the latest data available for the country'],
            'unit': ['Year'],
            'source': ['Data processing']
        })
        codebook_df = pd.concat([codebook_df, new_row], ignore_index=True)

    codebook_df = codebook_df[codebook_df['column'].isin(df_latest.columns)].reset_index(drop=True)
    transformed_codebook = transform_column_names(codebook_df.copy(), is_codebook=True)
    rename_map = dict(zip(codebook_df['column'], transformed_codebook['column']))
    df_latest.rename(columns=rename_map, inplace=True)
    logger.info(f"Renamed columns: {rename_map}")
    return df_latest

# Apply transformations to column names
def transform_column_names(df, is_codebook=False):
    if is_codebook:
        columns = df['column'].tolist()
        units = df['unit'].tolist()
    else:
        columns = df.columns.tolist()
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
        logger.info(f"Transformed '{original_col_name}' to '{new_col_name}'")

    if is_codebook:
        df['column'] = new_columns
    else:
        df.columns = new_columns

    logger.info(f"Final column names: {df.columns.tolist()}")
    return df

# Main Function

def main():
    logger.info("Starting combined script.")
    download_datasets()
    config = load_or_create_config()

    df = load_main_dataset()
    df_filtered = filter_main_dataset(df, config)
    codebook_df = load_codebook()

    # Apply transformations to the codebook
    codebook_df = apply_transformations(codebook_df)

    # Process for `processed_energy_data.csv`
    df_filtered = filter_year_range(df_filtered, config)
    df_latest = prioritize_active_year(df_filtered, config)

    # Fill missing GDP data using World Bank API
    df_latest = fill_gdp_using_world_bank(df_latest, config['active_year'], config['previousYearRange'])

    # Apply unit conversions before renaming columns
    df_latest = apply_unit_conversion_script(df_latest, codebook_df)

    # Update codebook units after conversions
    codebook_df = update_codebook_units_after_conversion(codebook_df)

    # Rename columns to include updated units (e.g., 'kWh')
    df_latest = rename_columns(df_latest, codebook_df)

    # Round numeric columns after all transformations are applied
    df_latest = round_numeric_columns(df_latest)

    # Save processed energy data
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.to_csv(output_path, index=False)
    logger.info(f"Processed energy data saved to {output_path}")

    # Process for timeline data `processed_energy_data_XXXX_YYYY.csv`
    timeline_start_year = config['timeline_start_year']
    timeline_end_year = config['timeline_end_year']
    timeline_columns_to_keep = config['timeline_columns_to_keep']
    df_timeline = filter_dataset_by_year_range(df, timeline_start_year, timeline_end_year)
    df_timeline = filter_dataset_columns(df_timeline, timeline_columns_to_keep)

    # Apply unit conversions to timeline dataset
    df_timeline = apply_unit_conversion_script(df_timeline, codebook_df)

    # Rename columns for timeline dataset after unit conversions
    df_timeline = rename_columns(df_timeline, codebook_df)

    # Round numeric columns for timeline dataset
    df_timeline = round_numeric_columns(df_timeline)

    # Save timeline processed energy data
    timeline_output_path = os.path.join(output_dir, f'processed_energy_data_{timeline_start_year}_{timeline_end_year}.csv')
    df_timeline.to_csv(timeline_output_path, index=False)
    logger.info(f"Timeline processed energy data saved to {timeline_output_path}")

if __name__ == "__main__":
    main()
