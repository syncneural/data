# Step 1: Import required libraries
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CodebookUpdater")

# Step 2: Load the codebook dataset
def load_codebook():
    return pd.read_csv('owid-energy-codebook.csv')

# Step 3: Filter codebook dataset for relevant rows and output with all columns
def filter_codebook(codebook_df):
    rows_to_keep = [
        "biofuel_electricity",
        "carbon_intensity_elec",
        "coal_electricity",
        "electricity_demand",
        "electricity_generation",
        "fossil_electricity",
        "low_carbon_electricity",
        "nuclear_electricity",
        "oil_electricity",
        "renewables_electricity",
        "solar_electricity",
        "wind_electricity"
    ]
    filtered_codebook = codebook_df[codebook_df['column'].isin(rows_to_keep)]
    return filtered_codebook

# Step 4: Save the filtered codebook dataset
def save_filtered_codebook(filtered_codebook):
    output_path = 'output/filtered_codebook.csv'
    filtered_codebook.to_csv(output_path, index=False)
    logger.info(f"Filtered codebook saved to {output_path}")

# Step 5: Main function
def main():
    codebook_df = load_codebook()
    filtered_codebook = filter_codebook(codebook_df)
    save_filtered_codebook(filtered_codebook)

# Run main function
if __name__ == "__main__":
    main()
