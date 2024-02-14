from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from delta import *
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'gcp_key_compras_bucket.json'

gcs_bucket = "compras-bucket"

# Create a Spark session with Delta Lake and GCS configurations
spark = (SparkSession.builder 
            .appName("DeltaLakeGCSExample") 
            .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") 
            .config("spark.sql.repl.eagerEval.enabled", True)
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") 
            .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")   # Specify GCS log store implementation
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")   # Specify GCS file system implementation
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")   # Specify GCS abstract file system implementation
            .getOrCreate())


def pdm_landing_to_bronze():
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load(f"gs://{gcs_bucket}/landing/pdm")
            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/bronze/pdm"))


def pdm_bronze_to_silver():
    return (spark
            .read
            .format("delta")
            .load(f"gs://{gcs_bucket}/bronze/pdm")
            .drop_duplicates(["Código"])
            .withColumn("codigoClasse", F.split('Classe', ':').getItem(0))
            .withColumn("descricaoClasse", F.split('Classe', ':').getItem(1))
            .withColumn("descricaoClasse", F.ltrim('descricaoClasse'))
            .drop('Classe')
            .withColumnRenamed("Código", "codigoPdm")
            .withColumnRenamed("Descrição", "descricaoPdm")
            .withColumn("descricaoPdm", F.trim('descricaoPdm'))
            .withColumn("descricaoClasse", F.trim('descricaoClasse'))            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/silver/pdm"))


def classes_landing_to_bronze():
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load(f"gs://{gcs_bucket}/landing/classes")
            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/bronze/classes"))

def classes_bronze_to_silver():
    return (spark
            .read
            .format("delta")
            .load(f"gs://{gcs_bucket}/bronze/classes")
            .withColumnRenamed("Código", "codigoClasse")
            .withColumnRenamed("Descricao", "descricaoClasse")
            .withColumn("codigoGrupo", F.split('Grupo', ':').getItem(0))
            .withColumn("descricaoGrupo", F.split('Grupo', ':').getItem(1))
            .withColumn("descricaoGrupo", F.trim('descricaoGrupo'))
            .withColumn("descricaoClasse", F.trim('descricaoClasse'))
            .drop("Grupo")
            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/silver/classes"))


def grupos_landing_to_bronze():
    return (spark
            .read
            .format("csv")
            .option("header", True)
            .load(f"gs://{gcs_bucket}/landing/grupos")
            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/bronze/grupos"))


def material_landing_to_bronze():
    return (spark
            .read
            .format("json")
            .load(f"gs://{gcs_bucket}/landing/material")
            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/bronze/material")
            )

def material_bronze_to_silver():
    df_material = (spark
                   .read
                   .format("delta")
                   .load(f"gs://{gcs_bucket}/bronze/material"))
    df_classes = (spark
                  .read
                  .format('delta')
                  .load(f"gs://{gcs_bucket}/silver/classes")
                  )
    return (df_material
            .join(F.broadcast(df_classes), ['codigoClasse'], "inner")
            .select('codigoItem', 
                    'codigoPdm', 
                    'nomePdm', 
                    'codigoClasse', 
                    'descricaoClasse', 
                    'codigoGrupo', 
                    'descricaoGrupo', 
                    'itemSuspenso', 
                    'itemSustentavel', 
                    'statusItem', 
                    'buscaItemCaracteristica')
            .write
            .format("delta")
            .mode("overwrite")
            .save(f"gs://{gcs_bucket}/silver/material")
            )


# classes_landing_to_bronze()
# grupos_landing_to_bronze()
# material_landing_to_bronze()

material_bronze_to_silver()