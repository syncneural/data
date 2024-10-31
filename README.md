Certainly! Below is the **combined `README.md`** that integrates both the functional programming approach using **Polars** and the detailed repository overview you provided. This comprehensive README includes all necessary scripts, configuration files, workflows, logging setups, and testing instructions to ensure a robust, efficient, and maintainable energy data processing pipeline.

---

# Energy Data Processing Pipeline

![Energy Flow Diagram](https://cdn.prod.website-files.com/66aa32062a320c20553f479e/66ec37b54c462ef070ba598a_maldives34.avif){: style="float: right; width: 500px; height: 500px; margin-left: 20px;" alt="Energy Flow Diagram"}

This repository contains scripts and workflows designed to process energy data and update the associated codebook used for energy analysis. The goal of these scripts and automation tools is to provide meaningful insights into renewable energy usage and trends by collecting, cleaning, and enhancing energy datasets. Leveraging **functional programming principles** and the high-performance **Polars** library, this pipeline ensures efficient data processing, maintainability, and scalability.

## Table of Contents

- [Project Structure](#project-structure)
- [Files Included](#files-included)
  - [Python Scripts](#python-scripts)
  - [Configuration Files](#configuration-files)
  - [Output Files](#output-files)
  - [GitHub Actions Workflow](#github-actions-workflow)
- [Automated Updates](#automated-updates)
- [Data Sources](#data-sources)
- [How to Use](#how-to-use)
  - [Running the Workflow Manually](#running-the-workflow-manually)
  - [Workflow Outputs](#workflow-outputs)
  - [Local Usage](#local-usage)
- [Testing](#testing)
- [Logging](#logging)
- [Functional Programming Principles Applied](#functional-programming-principles-applied)
- [Performance Optimization with Polars](#performance-optimization-with-polars)
- [Future Enhancements](#future-enhancements)
- [Support](#support)

---

## Project Structure

```
your-repo/
│
├── scripts/
│   ├── energy_data_processing.py
│   ├── update_codebook.py
│   └── utils.py
│
├── tests/
│   └── test_utils.py
│
├── .github/
│   └── workflows/
│       └── process_energy_data.yml
│
├── output/
│   ├── processed_energy_data.csv
│   └── codebook.csv
│
├── config.yaml
├── requirements.txt
└── README.md
```

---

## Files Included

### Python Scripts

- **`energy_data_processing.py`**:  
  This script processes energy data from Our World in Data (OWID). It downloads the raw energy dataset, applies necessary filtering and transformations, fills in missing GDP data using the World Bank API, renames columns for clarity, rounds numeric columns, and saves the processed dataset as `processed_energy_data.csv`. Key tasks include:
  
  - Downloading raw data.
  - Applying unit conversions.
  - Filtering for key metrics and relevant years.
  - Renaming columns to be more user-friendly.
  - Filling missing GDP data.
  - Rounding numeric columns.
  - Saving the final cleaned dataset.

- **`update_codebook.py`**:  
  This script updates the energy codebook by filtering for selected metrics and synchronizing it with the processed data. It ensures that any new derived metrics are added to the codebook and that the codebook remains consistent with the processed dataset. The updated codebook is saved as `codebook.csv`.

- **`utils.py`**:  
  Contains pure, reusable utility functions for transforming column names, applying unit conversions, and updating the codebook. Embraces functional programming principles by ensuring immutability and purity.

### Configuration Files

- **`config.yaml`**:  
  This configuration file contains settings used across both scripts, such as:
  
  - `columns_to_keep`: Specifies the columns that are to be retained from the energy dataset.
  - `active_year` and `previousYearRange`: Specifies the range of years to be used for analysis.
  
  *Note: This file is automatically created with default values if it doesn't exist.*

### Output Files

![Car Park Solar Installation](https://cdn.prod.website-files.com/66aa32062a320c20553f479e/66ec38dbf4d01031a6a1b9cf_carpark.avif){: style="float: right; width: 500px; height: 500px; margin-left: 20px;" alt="Car Park Solar Installation"}

- **`output/processed_energy_data.csv`**:  
  Contains the processed and filtered energy data, ready for further analysis or import into spreadsheets such as Google Sheets.

- **`output/codebook.csv`**:  
  Contains the filtered energy codebook, focusing on key metrics of interest, providing context and descriptions for each metric.

### GitHub Actions Workflow

- **`.github/workflows/process_energy_data.yml`**:  
  Defines a GitHub Actions workflow that automates the execution of both `energy_data_processing.py` and `update_codebook.py`. The workflow is set up to run automatically on a weekly basis (every Sunday at midnight UTC) or can be manually triggered. The workflow completes the following tasks:
  
  - Checks out the repository.
  - Sets up a Python environment.
  - Installs dependencies.
  - Runs the scripts to process energy data and update the codebook.
  - Commits and pushes any changes to the repository.

---

## Automated Updates

This repository includes a workflow that runs every Sunday at midnight or can be manually triggered to ensure data is always up to date:

- **Processed Data**: The latest energy data is processed and saved as `output/processed_energy_data.csv`.
- **Codebook**: The updated codebook with relevant metrics is saved as `output/codebook.csv`.
- **README.md**: This README file is kept updated by the workflow to reflect changes to the output files and updates to the scripts.

![Wind and Solar Farm](https://cdn.prod.website-files.com/66aa32062a320c20553f479e/66ec295ac55afcb693497d48_windandsolarfarm-p-1080.avif){: style="float: right; width: 500px; height: 500px; margin-left: 20px;" alt="Wind and Solar Farm"}

---

## Data Sources

- **Open World Data (OWID)**:  
  The energy data is sourced from [Our World in Data](https://github.com/owid/energy-data). This dataset includes key energy metrics for different countries across various years, providing insight into renewable energy usage, fossil fuel consumption, and overall energy demand.

- **World Bank API**:  
  Missing GDP data is filled using the [World Bank API](https://data.worldbank.org/), ensuring complete and accurate economic context for each country in the dataset.

---

## How to Use

### Running the Workflow Manually

To run the workflow manually, navigate to the **Actions** tab in this repository, select the workflow named **Process Energy Data and Update Codebook**, and click on **Run workflow**. This will initiate the workflow to download, process, and update the datasets.

### Workflow Outputs

After the workflow completes, the following outputs will be updated in the `output/` folder:

- **`processed_energy_data.csv`**:  
  Contains the latest processed energy dataset, cleaned and enhanced for easy analysis.

- **`codebook.csv`**:  
  The filtered codebook dataset focusing on key energy metrics, providing clarity on each metric's meaning and units.

### Local Usage

If you wish to run the scripts locally, follow these steps:

1. **Clone the Repository**:

   ```sh
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```

2. **Create a Virtual Environment (Optional but Recommended)**:

   ```sh
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies**:

   ```sh
   pip install -r requirements.txt
   ```

4. **Run Energy Data Processing Script**:

   ```sh
   python scripts/energy_data_processing.py
   ```

   - This script will process the energy data and save the results in the `output/` directory.

5. **Run Codebook Update Script**:

   ```sh
   python scripts/update_codebook.py
   ```

   - This script will update the codebook based on the processed data and save it in the `output/` directory.

---

## Testing

### Unit Tests

Unit tests are provided to ensure the utility functions behave as expected. The tests are located in the `tests/` directory and can be run using `pytest`.

1. **Install Testing Dependencies**:

   Ensure `pytest` is installed by adding it to `requirements.txt` or installing separately.

   ```sh
   pip install pytest
   ```

2. **Run Tests**:

   ```sh
   pytest tests/
   ```

   - This command runs all unit tests to verify the functionality of utility functions.

### Example Tests

```python
# tests/test_utils.py

import pytest
import polars as pl
from scripts.utils import (
    transform_column_names,
    apply_unit_conversion,
    update_codebook_units,
    apply_transformations
)

def test_transform_column_names_codebook():
    input_df = pl.DataFrame({
        'column': ['energy_consumption', 'emissions'],
        'description': ['Energy Consumption in TWh', 'Emissions in million tonnes'],
        'unit': ['TWh', 'million tonnes'],
        'source': ['Source A', 'Source B']
    })
    transformed_df = transform_column_names(input_df, is_codebook=True)
    expected_columns = ['Energy Consumption kWh', 'Emissions tonnes']
    assert transformed_df["column"].to_list() == expected_columns

def test_transform_column_names_data():
    input_df = pl.DataFrame({
        'energy_consumption': [1, 2],
        'emissions': [3, 4]
    })
    transformed_df = transform_column_names(input_df, is_codebook=False)
    expected_columns = ['Energy Consumption kWh', 'Emissions tonnes']
    assert transformed_df.columns == expected_columns

def test_apply_unit_conversion():
    input_df = pl.DataFrame({
        'energy_consumption': [1, 2],
        'emissions': [3, 4],
        'population': [1000, 2000],
        'gdp': [50000, 60000]
    })
    codebook_df = pl.DataFrame({
        'column': ['energy_consumption', 'emissions'],
        'description': ['Energy Consumption in TWh', 'Emissions in million tonnes'],
        'unit': ['TWh', 'million tonnes'],
        'source': ['Source A', 'Source B']
    })
    converted_df = apply_unit_conversion(input_df, codebook_df)
    expected_df = pl.DataFrame({
        'energy_consumption': [1e9, 2e9],
        'emissions': [3e6, 4e6],
        'population': [1000, 2000],
        'gdp': [50000, 60000]
    })
    assert converted_df.frame_equal(expected_df)

def test_update_codebook_units():
    codebook_df = pl.DataFrame({
        'column': ['energy_consumption', 'emissions'],
        'description': ['Energy Consumption in TWh', 'Emissions in million tonnes'],
        'unit': ['TWh', 'million tonnes'],
        'source': ['Source A', 'Source B']
    })
    updated_codebook = update_codebook_units(codebook_df)
    expected_units = ['kilowatt-hours', 'million tonnes']
    assert updated_codebook["unit"].to_list() == expected_units

def test_apply_transformations():
    codebook_df = pl.DataFrame({
        'column': ['energy_consumption', 'emissions'],
        'description': ['Energy Consumption in TWh', 'Emissions in million tonnes'],
        'unit': ['TWh', 'million tonnes'],
        'source': ['Source A', 'Source B']
    })
    transformed_codebook = apply_transformations(codebook_df)
    expected_descriptions = [
        'Energy Consumption in kilowatt-hours',
        'Emissions in tonnes (Measured as a percentage fraction of 1, e.g., 0.32 = 32%)'
    ]
    # Since 'emissions' doesn't contain '%', the description should only have "tonnes"
    # Adjust the expected accordingly
    expected_descriptions = [
        'Energy Consumption in kilowatt-hours',
        'Emissions in tonnes'
    ]
    assert transformed_codebook["description"].to_list() == expected_descriptions
    assert transformed_codebook["unit"].to_list() == ['kilowatt-hours', 'tonnes']
```

---

## Logging

- **Logging Configuration:**
  - Both `energy_data_processing.py` and `update_codebook.py` utilize Python's `logging` module for detailed logs.
  - Logs are output to the console with varying levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`).

- **Log Levels:**
  - **DEBUG:** Detailed information, typically of interest only when diagnosing problems.
  - **INFO:** Confirmation that things are working as expected.
  - **WARNING:** An indication that something unexpected happened.
  - **ERROR:** Due to a more serious problem, the software has not been able to perform some function.

---

## Functional Programming Principles Applied

Functional Programming emphasizes:

- **Pure Functions:** Functions that return the same output for the same input and have no side effects.
- **Immutability:** Avoiding changes to data in place; instead, returning new data structures.
- **Function Composition:** Building complex operations by combining simple, reusable functions.
- **Higher-Order Functions:** Utilizing functions that take other functions as arguments or return them.

### **Key Functional Enhancements:**

1. **Pure Functions:**
   - Each function performs a specific, isolated task without side effects.
   - DataFrames are not modified in place; instead, new DataFrames are returned.

2. **Immutability:**
   - Functions like `apply_unit_conversion`, `update_codebook_units`, and `transform_column_names` create and return new DataFrames rather than altering the originals.

3. **Function Composition:**
   - The `main` function orchestrates the data processing pipeline by sequentially applying functions.

4. **Type Annotations:**
   - Added type hints for better readability and tooling support.

5. **Error Handling:**
   - Incorporated `try-except` blocks and proper logging to handle potential errors gracefully.

---

## Performance Optimization with Polars

### **Why Polars?**

- **Speed:** Polars often outperforms Pandas by orders of magnitude, especially on multi-core systems.
- **Memory Efficiency:** It uses Apache Arrow under the hood, ensuring efficient memory usage.
- **Lazy Evaluation:** Allows for query optimizations by delaying execution until necessary.
- **Threading:** Utilizes multi-threading out of the box for parallel computations.

### **Benchmarking Example**

Here's a brief benchmark comparing Polars and Pandas for reading a large CSV file:

```python
import pandas as pd
import polars as pl
import time

# Define a large CSV path
csv_path = 'large_energy_data.csv'

# Benchmark Pandas
start_time = time.time()
df_pandas = pd.read_csv(csv_path)
end_time = time.time()
print(f"Pandas read_csv took {end_time - start_time:.2f} seconds.")

# Benchmark Polars
start_time = time.time()
df_polars = pl.read_csv(csv_path)
end_time = time.time()
print(f"Polars read_csv took {end_time - start_time:.2f} seconds.")
```

**Expected Output:**

```
Pandas read_csv took 30.00 seconds.
Polars read_csv took 3.00 seconds.
```

*Note: Actual times will vary based on system specifications and dataset size.*

---

## Future Enhancements

1. **Advanced Function Composition:**
   - Integrate libraries like `toolz` or `functools` for more sophisticated function compositions.

2. **Asynchronous Processing:**
   - Utilize asynchronous requests for fetching GDP data to further enhance performance.

3. **Automated Testing:**
   - Expand test coverage to include more edge cases and scenarios.

4. **Documentation:**
   - Enhance documentation with more detailed explanations and usage examples.

5. **Error Handling Enhancements:**
   - Implement more robust error handling mechanisms to gracefully handle unexpected scenarios.

6. **Performance Monitoring:**
   - Incorporate performance monitoring tools to continuously assess and optimize pipeline efficiency.

---

## Support

If you encounter any issues or have questions, feel free to open an issue in the repository or contact the maintainer.

---

**Congratulations on setting up your high-performance, functional programming-based energy data processing pipeline! 🎉**

If you have any further questions or need assistance with additional features, optimizations, or troubleshooting, don't hesitate to reach out.

---

## Additional Information

### **Requirements**

Ensure you have the following installed:

- Python 3.10 or higher
- Required Python packages listed in `requirements.txt`

### **Installation**

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```

2. **Create a Virtual Environment (Optional but Recommended):**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

### **Running the Scripts Locally**

1. **Process Energy Data:**

   ```bash
   python scripts/energy_data_processing.py
   ```

2. **Update the Codebook:**

   ```bash
   python scripts/update_codebook.py
   ```

---

## Contact

For any queries or support, please contact [info -at- syncneural.net](mailto:info -at- syncneural.net).

---

# License

This project is not currently licensed for reuse

---

# Acknowledgments

- [Our World in Data](https://github.com/owid/energy-data) for the comprehensive energy datasets.
- [World Bank API](https://data.worldbank.org/) for providing reliable GDP data.

---

# Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

---

# Changelog

- **v1.0.0**: Initial release with energy data processing and codebook update scripts.
- **v1.1.0**: Refactored scripts to use Polars and adopt functional programming principles.
- **v1.2.0**: Added unit tests and enhanced logging for better traceability.

---

Feel free to customize this README further to better fit your project's specific needs and to include any additional information that might be relevant to users or contributors.
