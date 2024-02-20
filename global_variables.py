from pyspark.sql import SparkSession

classes_endpoint = "https://compras.dados.gov.br/materiais/v1/classes.csv"
grupos_endpoint = "https://compras.dados.gov.br/materiais/v1/grupos.csv"
pdms_endpoint = "https://compras.dados.gov.br/materiais/v1/pdms.csv"
material_endpoint = "https://cnbs.estaleiro.serpro.gov.br/cnbs-api/material/v1"


gcs_bucket = "compras-bucket"
pdm_landing_path = f"gs://{gcs_bucket}/landing/pdm"
pdm_bronze_path = f"gs://{gcs_bucket}/bronze/pdm"
pdm_silver_path = f"gs://{gcs_bucket}/silver/pdm"

classes_landing_path = f"gs://{gcs_bucket}/landing/classes"
classes_bronze_path = f"gs://{gcs_bucket}/bronze/classes"
classes_silver_path = f"gs://{gcs_bucket}/silver/classes"

grupos_landing_path = f"gs://{gcs_bucket}/landing/grupos"
grupos_bronze_path = f"gs://{gcs_bucket}/bronze/grupos"

material_landing_path = f"gs://{gcs_bucket}/landing/material"
material_bronze_path = f"gs://{gcs_bucket}/bronze/material"
material_silver_path = f"gs://{gcs_bucket}/silver/material"
material_pdm_gold_path = f"gs://{gcs_bucket}/gold/material_pdm_agg"


def init_spark_session() -> SparkSession:
    """ Creates a Spark session with Delta Lake and GCS configurations
    """
    return (SparkSession.builder 
            .appName("ComprasCatalogoSparkApp") 
            .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") 
            .config("spark.sql.repl.eagerEval.enabled", True)
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") 
            .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")   # Specify GCS log store implementation
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")   # Specify GCS file system implementation
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")   # Specify GCS abstract file system implementation
            .getOrCreate())