# Import necessary libraries
from great_expectations.data_context import get_context
from great_expectations.dataset import SparkDFDataset
from utils.config import *


def check_landing(spark: SparkSession):
    df = spark.read.format("parquet").load(f"{material_landing_path}")

    # Create a Great Expectations SparkDFDataset
    ge_df = SparkDFDataset(df, batch_kwargs={"ge_batch_id": 1})

    columns_needed = [
        "codigoClasse",
        "codigoItem",
        "codigoPdm",
        "itemExclusivoUasgCentral",
        "itemSuspenso",
        "itemSustentavel",
        "nomePdm",
        "statusItem",
        "buscaItemCaracteristica",
    ]

    for column in columns_needed:
        try:
            assert ge_df.expect_column_to_exist(
                column
            ).success, f"Column {column} not found: FAILED"
            print(f"Column {column} exists : PASSED")
        except AssertionError as e:
            print(e)
