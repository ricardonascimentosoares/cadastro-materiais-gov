# Import necessary libraries
from great_expectations.data_context import get_context
from great_expectations.dataset import SparkDFDataset
from utils.config import *


def check_landing(spark):
    df = spark.read.format("csv").option("header", True).load(classes_landing_path)

    # Create a Great Expectations SparkDFDataset
    ge_df = SparkDFDataset(df, batch_kwargs={"ge_batch_id": 1})

    columns_needed = ["Código", "Descricao", "Grupo"]

    for column in columns_needed:
        try:
            assert ge_df.expect_column_to_exist(
                column
            ).success, f"Column {column} not found: FAILED"
            print(f"Column {column} exists : PASSED")
        except AssertionError as e:
            print(e)

    try:
        assert ge_df.expect_column_values_to_not_be_null(
            "Código"
        ).success, f"Column {column} has null values: FAILED"
        print(f"Column Código does not have null values : PASSED")
    except AssertionError as e:
        print(e)
