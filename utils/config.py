from pyspark.sql import SparkSession

classes_endpoint = "https://compras.dados.gov.br/materiais/v1/classes.csv"
grupos_endpoint = "https://compras.dados.gov.br/materiais/v1/grupos.csv"
pdms_endpoint = "https://compras.dados.gov.br/materiais/v1/pdms.csv"
material_endpoint = "https://cnbs.estaleiro.serpro.gov.br/cnbs-api/material/v1"

# The name of bucket from GCP
gcs_bucket = "compras-bucket"

# Specify the path to your service account key file
key_path = "utils/gcp_key_compras_bucket.json"

pdm_landing_path = f"gs://{gcs_bucket}/landing/pdm"
pdm_bronze_path = f"gs://{gcs_bucket}/bronze/pdm"
pdm_silver_path = f"gs://{gcs_bucket}/silver/pdm"

classes_landing_path = f"gs://{gcs_bucket}/landing/classes"
classes_bronze_path = f"gs://{gcs_bucket}/bronze/classes"
classes_silver_path = f"gs://{gcs_bucket}/silver/classes"

material_landing_path = f"gs://{gcs_bucket}/landing/material_v2"
material_bronze_path = f"gs://{gcs_bucket}/bronze/material"
material_silver_path = f"gs://{gcs_bucket}/silver/material"
material_pdm_gold_path = f"gs://{gcs_bucket}/gold/material_pdm_agg"
material_char_detail_path = f"gs://{gcs_bucket}/gold/material_char_detail"

output_path = f"gs://{gcs_bucket}/output"


def init_spark_session() -> SparkSession:
    """Creates a Spark session with Delta Lake and GCS configurations"""
    return (
        SparkSession.builder.appName("ComprasCatalogoSparkApp")
        .config("spark.driver.memory", "2g")
        .master("local[*]")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.executor.heartbeatInterval", "6000000")
        .config("spark.network.timeout", "10000000")
        .config(
            "spark.jars",
            "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        )
        .config("spark.sql.repl.eagerEval.enabled", True)
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
        .config(
            "spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore"
        )  # Specify GCS log store implementation
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )  # Specify GCS file system implementation
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )  # Specify GCS abstract file system implementation
        # .config("spark.hadoop.fs.gs.inputstream.connect.timeout", "600000")  # 60 seconds
        # .config("spark.hadoop.fs.gs.inputstream.read.timeout", "600000")  # 60 seconds
        .getOrCreate()
    )
