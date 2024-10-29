# Repository Overview

This repository contains scripts and workflows designed to process energy data and update the associated codebook used for energy analysis. The goal of these scripts and automation tools is to provide meaningful insights into renewable energy usage and trends by collecting, cleaning, and enhancing energy datasets.

![Energy Flow Diagram](https://syncneural.net/images/energy-flow-diagram.png)

## Files Included

### Python Scripts
- **energy_data_processing.py**: This script is responsible for processing energy data from Open World Data (OWID). It downloads the raw energy dataset, applies necessary filtering and transformations, fills in missing GDP data using the World Bank API, and finally saves the processed dataset as `processed_energy_data.csv`. The script's main tasks include:
  - Downloading raw data.
  - Pre-caching GDP data for missing entries.
  - Filtering for key metrics and relevant years.
  - Renaming columns to make them more user-friendly.
  - Saving the final cleaned dataset.

- **update_codebook.py**: This script processes the energy codebook to filter for selected metrics and outputs a filtered version called `codebook.csv`. The codebook provides detailed information about the dataset's columns, including units and descriptions.

### Configuration Files
- **config.yaml**: This configuration file contains settings used across both scripts, such as:
  - `columns_to_keep`: Specifies the columns that are to be retained from the energy dataset.
  - `active_year` and `previousYearRange`: Specifies the range of years to be used for analysis.

### Output Files
- **output/processed_energy_data.csv**: Contains the processed and filtered energy data, ready for further analysis or import into spreadsheets such as Google Sheets.
- **output/codebook.csv**: Contains the filtered energy codebook, focusing on key metrics of interest, providing context and descriptions for each metric.

![Processed Energy Data Overview](https://syncneural.net/images/processed-energy-data.png)

### GitHub Actions Workflow
- **.github/workflows/process_energy_data.yml**: This file defines a GitHub Actions workflow that automates the execution of both `energy_data_processing.py` and `update_codebook.py`. The workflow is set up to run automatically on a weekly basis (every Sunday at midnight) or can be manually triggered. The workflow completes the following tasks:
  - Checks out the repository.
  - Sets up a Python environment.
  - Installs dependencies.
  - Runs the scripts to process energy data and update the codebook.
  - Commits and pushes any changes to the repository.

## Automated Updates

This repository includes a workflow that runs every Sunday at midnight or can be manually triggered to ensure data is always up to date:
- **Processed Data**: The latest energy data is processed and saved as `output/processed_energy_data.csv`.
- **Codebook**: The updated codebook with relevant metrics is saved as `output/codebook.csv`.
- **README.md**: This README file is kept updated by the workflow to reflect changes to the output files and updates to the scripts.

## Data Sources

- **Open World Data (OWID)**: The energy data is sourced from [Our World in Data](https://github.com/owid/energy-data). This dataset includes key energy metrics for different countries across various years, providing insight into renewable energy usage, fossil fuel consumption, and overall energy demand.
- **World Bank API**: Missing GDP data is filled using the [World Bank API](https://data.worldbank.org/), ensuring complete and accurate economic context for each country in the dataset.

## How to Use

### Running the Workflow Manually
To run the workflow manually, navigate to the **Actions** tab in this repository, select the workflow named **Process Energy Data and Update Codebook**, and click on **Run workflow**. This will initiate the workflow to download, process, and update the datasets.

### Workflow Outputs
After the workflow completes, the following outputs will be updated in the `output/` folder:
- **`processed_energy_data.csv`**: Contains the latest processed energy dataset, cleaned and enhanced for easy analysis.
- **`codebook.csv`**: The filtered codebook dataset focusing on key energy metrics, providing clarity on each metric's meaning and units.

### Local Usage
If you wish to run the scripts locally, follow these steps:
1. **Clone the Repository**: 
   ```sh
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```
2. **Install Dependencies**:
   Make sure you have Python 3.8+ installed, then run:
   ```sh
   pip install -r requirements.txt
   ```
   The dependencies include `pandas`, `requests`, and `pyyaml`.
3. **Run the Scripts**:
   - To process energy data, run:
     ```sh
     python scripts/energy_data_processing.py
     ```
   - To update the codebook, run:
     ```sh
     python scripts/update_codebook.py
     ```

## Dependencies
The Python scripts in this repository rely on the following libraries:
- **pandas**: For data processing and manipulation.
- **requests**: To retrieve data from external sources like OWID and World Bank.
- **pyyaml**: For handling configurations from the `config.yaml` file.

These libraries are automatically installed as part of the GitHub Actions workflow, or you can install them locally via `pip install -r requirements.txt`.

## Contributing
If you wish to contribute to this project, feel free to open a pull request or issue. Contributions can include new features, bug fixes, or improvements to the documentation. Please ensure you follow the existing coding structure and provide detailed comments for any new additions.

## Contact
For any questions or issues, please reach out via the issues tab in this repository. Your feedback and contributions are welcome!

![Energy Analysis Illustration](https://syncneural.net/images/energy-analysis-illustration.png)

