# scripts/energy_data_processing.py

import os
import logging
import yaml
import polars as pl
import requests
from concurrent.futures import ThreadPoolExecutor
from utils import (
    transform_column_names,
    apply_unit_conversion,
    update_codebook_units,
    apply_transformations
)

logger = logging.getLogger("EnergyDataProcessor")
logger.setLevel(logging.INFO)  # Set to INFO or DEBUG as needed

# Create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)  # Set to DEBUG for detailed logs

# Create formatter and add it to the handlers
formatter = logging.Formatter('[%(levelname)s] %(name)s - %(message)s')
ch.setFormatter(formatter)

# Add the handlers to the logger
if not logger.hasHandlers():
    logger.addHandler(ch)

def download_datasets(data_path: str = 'owid-energy-data.csv', codebook_path: str = 'owid-energy-codebook.csv') -> None:
    """
    Downloads the energy data and codebook if they do not exist locally.
    
    Args:
        data_path (str): Path to save the energy data CSV.
        codebook_path (str): Path to save the codebook CSV.
    """
    if not os.path.exists(data_path):
        data_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv'
        df = pl.read_csv(data_url)
        df.write_csv(data_path)
        logger.info("Downloaded owid-energy-data.csv successfully.")
    else:
        logger.info("owid-energy-data.csv already exists.")

    if not os.path.exists(codebook_path):
        codebook_url = 'https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-codebook.csv'
        codebook_df = pl.read_csv(codebook_url)
        codebook_df.write_csv(codebook_path)
        logger.info("Downloaded owid-energy-codebook.csv successfully.")
    else:
        logger.info("owid-energy-codebook.csv already exists.")

def load_codebook(codebook_path: str = 'owid-energy-codebook.csv') -> pl.DataFrame:
    """
    Loads the codebook from a CSV file.
    
    Args:
        codebook_path (str): Path to the codebook CSV.
        
    Returns:
        pl.DataFrame: The codebook DataFrame.
    """
    return pl.read_csv(codebook_path)

def load_or_create_config(config_path: str = 'config.yaml') -> dict:
    """
    Loads the configuration from a YAML file or creates a default config.
    
    Args:
        config_path (str): Path to the config YAML file.
        
    Returns:
        dict: Configuration dictionary.
    """
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
    else:
        config = {
            'columns_to_keep': ['country', 'year', 'iso_code', 'population', 'gdp'],
            'active_year': 2022,
            'previousYearRange': 5,
            'force_update': True
        }
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
            logger.info(f"Created default configuration at {config_path}")
    return config

def load_main_dataset(data_path: str = 'owid-energy-data.csv') -> pl.DataFrame:
    """
    Loads the main energy dataset from a CSV file.
    
    Args:
        data_path (str): Path to the energy data CSV.
        
    Returns:
        pl.DataFrame: The main energy data DataFrame.
    """
    return pl.read_csv(data_path)

def filter_main_dataset(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Filters the main dataset based on the configuration.
    
    Args:
        df (pl.DataFrame): The main energy data DataFrame.
        config (dict): Configuration dictionary.
        
    Returns:
        pl.DataFrame: Filtered DataFrame.
    """
    return df.select(config['columns_to_keep']).filter(pl.col("population").is_not_null())

def filter_year_range(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Filters the DataFrame to include only years within the specified range.
    
    Args:
        df (pl.DataFrame): The DataFrame to filter.
        config (dict): Configuration dictionary.
        
    Returns:
        pl.DataFrame: Year-filtered DataFrame.
    """
    active_year = config['active_year']
    previous_year_range = config['previousYearRange']
    start_year = active_year - previous_year_range
    return df.filter(pl.col("year") >= start_year)

def prioritize_active_year(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Prioritizes the active year for each country.
    
    Args:
        df (pl.DataFrame): The DataFrame to prioritize.
        config (dict): Configuration dictionary.
        
    Returns:
        pl.DataFrame: DataFrame with the latest data per country.
    """
    return df.sort(['country', 'iso_code', 'year'], descending=[False, False, True]) \
             .unique(subset=['country', 'iso_code'], keep='first') \
             .with_column(pl.col("year").alias("latest_data_year"))

def fetch_gdp_data(country_code: str, start_year: int, end_year: int) -> dict:
    """
    Fetches GDP data for a specific country and year range from the World Bank API.
    
    Args:
        country_code (str): ISO code of the country.
        start_year (int): Start year for GDP data.
        end_year (int): End year for GDP data.
        
    Returns:
        dict: Dictionary with year as key and GDP as value.
    """
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?date={start_year}:{end_year}&format=json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        gdp_data = {}
        if len(data) > 1 and data[1]:
            for item in data[1]:
                gdp_value = item['value']
                year = int(item['date'])
                if gdp_value is not None:
                    gdp_data[year] = gdp_value
        else:
            logger.warning(f"No GDP data found for {country_code}")
        return gdp_data
    except requests.RequestException as e:
        logger.error(f"Error fetching GDP data for {country_code}: {e}")
        return {}

def fill_gdp_using_world_bank(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """
    Fills missing GDP values using data from the World Bank API.
    
    Args:
        df (pl.DataFrame): The DataFrame with potential missing GDP values.
        config (dict): Configuration dictionary.
        
    Returns:
        pl.DataFrame: DataFrame with GDP values filled.
    """
    missing_gdp = df.filter(pl.col("gdp").is_null() & pl.col("iso_code").is_not_null())
    gdp_results = {}

    def fetch_and_store(row):
        country_code = row["iso_code"]
        country = row["country"]
        start_year = config['active_year'] - config['previousYearRange']
        end_year = config['active_year']
        gdp_data = fetch_gdp_data(country_code, start_year, end_year)
        if gdp_data:
            latest_year = max(gdp_data.keys())
            gdp_results[country] = {"gdp": gdp_data[latest_year], "latest_data_year": latest_year}
            logger.debug(f"Fetched GDP for {country} ({country_code}) for year {latest_year}: {gdp_data[latest_year]}")
        else:
            logger.warning(f"No GDP data available within the specified range for {country} ({country_code})")

    # Use ThreadPoolExecutor for parallel requests
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_and_store, missing_gdp.iter_rows(named=True))

    # Create a DataFrame from gdp_results
    if gdp_results:
        gdp_df = pl.DataFrame([
            {"country": country, "gdp": data["gdp"], "latest_data_year": data["latest_data_year"]}
            for country, data in gdp_results.items()
        ])
        # Join with the original DataFrame to fill in the missing values
        df_filled = df.join(gdp_df, on="country", how="left")
        df_filled = df_filled.with_columns([
            pl.coalesce([pl.col("gdp_y"), pl.col("gdp_x")]).alias("gdp"),
            pl.coalesce([pl.col("latest_data_year_y"), pl.col("latest_data_year_x")]).alias("latest_data_year")
        ]).drop(["gdp_x", "gdp_y", "latest_data_year_x", "latest_data_year_y"])
        logger.info(f"Filled GDP values for {len(gdp_results)} countries.")
        return df_filled
    else:
        logger.info("No GDP data was fetched to fill missing values.")
        return df

def round_numeric_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rounds specified numeric columns to zero decimal places.
    
    Args:
        df (pl.DataFrame): The DataFrame with numeric columns to round.
        
    Returns:
        pl.DataFrame: DataFrame with rounded numeric columns.
    """
    columns_to_round = ['population', 'gdp']
    kwh_columns = [col for col in df.columns if 'kwh' in col.lower()]
    carbon_intensity_columns = [col for col in df.columns if 'gco₂e/kwh' in col.lower() or 'gco2e/kwh' in col.lower()]
    
    columns_to_round.extend(kwh_columns)
    columns_to_round.extend(carbon_intensity_columns)
    columns_to_round = list(set(columns_to_round))
    
    for col in columns_to_round:
        if col in df.columns and df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            df = df.with_column(
                pl.col(col).round(0).cast(pl.Int64).alias(col)
            )
            logger.info(f"Rounded '{col}' to zero decimal places.")
    return df

def rename_columns(df: pl.DataFrame, codebook_df: pl.DataFrame) -> pl.DataFrame:
    """
    Renames DataFrame columns based on the codebook's transformed column names.
    
    Args:
        df (pl.DataFrame): The DataFrame with original column names.
        codebook_df (pl.DataFrame): The codebook DataFrame.
        
    Returns:
        pl.DataFrame: DataFrame with renamed columns.
    """
    # Ensure 'latest_data_year' is in codebook
    if 'latest_data_year' not in codebook_df['column'].to_list():
        latest_year_row = pl.DataFrame({
            'column': ['latest_data_year'],
            'description': ['Year of the latest data available for the country'],
            'unit': ['Year'],
            'source': ['Data processing']
        })
        codebook_df = codebook_df.vstack(latest_year_row)
        logger.debug("Added 'latest_data_year' to the codebook.")

    # Filter codebook to columns present in df
    codebook_filtered = codebook_df.filter(pl.col("column").is_in(df.columns))
    
    # Transform column names
    codebook_transformed = transform_column_names(codebook_filtered, is_codebook=True)
    
    # Create rename map
    rename_map = {
        old: new for old, new in zip(codebook_filtered["column"].to_list(), codebook_transformed["column"].to_list())
    }
    
    # Rename columns
    df_renamed = df.rename(rename_map)
    logger.info(f"Renamed columns: {rename_map}")
    return df_renamed

def main():
    logger.info("Starting energy data processing script.")
    download_datasets()

    config = load_or_create_config()
    df = load_main_dataset()
    df_filtered = filter_main_dataset(df, config)
    codebook_df = load_codebook()
    df_converted = apply_unit_conversion(df_filtered, codebook_df)
    codebook_updated = update_codebook_units(codebook_df)
    df_filtered = filter_year_range(df_converted, config)
    df_latest = prioritize_active_year(df_filtered, config)
    df_latest = fill_gdp_using_world_bank(df_latest, config)
    df_latest = rename_columns(df_latest, codebook_updated)
    df_latest = round_numeric_columns(df_latest)
    
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'processed_energy_data.csv')
    df_latest.write_csv(output_path)
    logger.info(f"Processed energy data saved to '{output_path}'")

if __name__ == "__main__":
    main()
