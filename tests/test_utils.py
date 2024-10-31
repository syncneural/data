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

