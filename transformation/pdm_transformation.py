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
    df_pdms = (
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
        .withColumn("descricaoPdm", F.regexp_replace("descricaoPdm", '"', ""))
        .withColumn("descricaoPdm", F.initcap("descricaoPdm"))
        .withColumn("descricaoClasse", F.initcap("descricaoClasse"))
    )

    df_classes = (
        spark.read.format("delta")
        .load(classes_silver_path)
        .select("codigoClasse", "codigoGrupo", "descricaoGrupo")
    )
    (
        df_pdms.join(df_classes, ["codigoClasse"], "left")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(pdm_silver_path)
    )


def export_data_to_excel(spark):
    print("Exporting data...")

    (spark.read.format("delta").load(pdm_silver_path)).toPandas().to_excel(
        output_path + "/pdm_list.xlsx"
    )
