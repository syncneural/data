# Step 2: Import required libraries
import pandas as pd
import requests
import logging
import threading
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EnergyDataProcessor")  # Updated logger name to make log messages more descriptive

# Configure pandas display settings for clarity
def configure_pandas():
    pd.set_option('display.float_format', '{:.0f}'.format)  # Show full numbers, no scientific notation
    pd.set_option('display.max_rows', 250)  # Display up to 5 rows for brevity
    pd.set_option('display.max_columns', None)  # Display all columns without truncation

# Set the active year and previous year range
active_year = 2022
previousYearRange = 5

# Pre-cache GDP data if it already exists
def load_cached_gdp_data():
    if os.path.exists("cached_gdp_data.csv"):
        return pd.read_csv("cached_gdp_data.csv", index_col=0).to_dict(orient='index')
    else:
        return {}

# Save cached GDP data
def save_cached_gdp_data(gdp_data):
    pd.DataFrame.from_dict(gdp_data, orient='index').to_csv("cached_gdp_data.csv")

# Fetch the GDP data for a specific country
# Added backoff strategy for retrying requests
def fetch_gdp_data_range(country_code, start_year, end_year, result_dict, retry=3):
    """
    Fetches GDP data for a given country across a specified year range in a single API call.

    Args:
        country_code (str): The ISO 3-letter country code.
        start_year (int): The starting year for the data range.
        end_year (int): The ending year for the data range.
        result_dict (dict): A dictionary to store the fetched GDP data.
        retry (int): Number of retries in case of failure.
    """
    year_range = f"{start_year}:{end_year}"
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?date={year_range}&format=json"
    backoff_time = 5
    while retry > 0:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            gdp_data = {}
            if len(data) > 1:
                for item in data[1]:
                    gdp_value = item['value']
                    year = int(item['date'])
                    gdp_data[year] = gdp_value
            result_dict[country_code] = gdp_data
            return
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch GDP data for {country_code} from World Bank API: {e}. Retries left: {retry-1}")
            retry -= 1
            time.sleep(backoff_time)  # Implement exponential backoff
            backoff_time *= 2
    result_dict[country_code] = None

# (Rest of the code remains unchanged)

# Main execution function
def main():
    configure_pandas()
    download_datasets()
    df = load_main_dataset()
    codebook_df = load_codebook()
    df_filtered = filter_relevant_columns(df)
    df_filtered = apply_unit_conversion(df_filtered, codebook_df)
    df_filtered = filter_year_range(df_filtered)
    df_latest = prioritize_active_year(df_filtered)

    # Start filling missing GDP data in parallel for each country
    gdp_results = load_cached_gdp_data()
    df_latest = fill_gdp_using_world_bank(df_latest, gdp_results)
    save_cached_gdp_data(gdp_results)

    df_latest = rename_columns(df_latest, codebook_df)

    # Save the final dataframe as CSV
    df_latest.to_csv('output/processed_energy_data.csv', index=False)
    logger.info("Final processed data saved to 'output/processed_energy_data.csv'")

# Run main function
if __name__ == "__main__":
    main()
