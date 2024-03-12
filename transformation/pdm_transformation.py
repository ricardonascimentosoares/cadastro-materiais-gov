import pyspark.sql.functions as F
from delta import *
from utils.config import *


def landing_to_bronze(spark):
    (
        spark.read.format("csv")
        .option("header", True)
        .load(pdm_landing_path)
        .write.format("delta")
        .mode("overwrite")
        .save(pdm_bronze_path)
    )


def bronze_to_silver(spark):
    (
        spark.read.format("delta")
        .load(pdm_bronze_path)
        .drop_duplicates(["Código"])
        .withColumn("codigoClasse", F.split("Classe", ":").getItem(0))
        .withColumn("descricaoClasse", F.split("Classe", ":").getItem(1))
        .withColumn("descricaoClasse", F.ltrim("descricaoClasse"))
        .drop("Classe")
        .withColumnRenamed("Código", "codigoPdm")
        .withColumnRenamed("Descrição", "descricaoPdm")
        .withColumn("descricaoPdm", F.trim("descricaoPdm"))
        .withColumn("descricaoClasse", F.trim("descricaoClasse"))
        .withColumn("descricaoPdm", F.initcap("descricaoPdm"))
        .withColumn("descricaoClasse", F.initcap("descricaoClasse"))
        .write.format("delta")
        .mode("overwrite")
        .save(pdm_silver_path)
    )
