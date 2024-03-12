import pyspark.sql.functions as F
from delta import *
from utils.config import *


def landing_to_bronze(spark):
    return (
        spark.read.format("csv")
        .option("header", True)
        .load(classes_landing_path)
        .write.format("delta")
        .mode("overwrite")
        .save(classes_bronze_path)
    )


def bronze_to_silver(spark):
    return (
        spark.read.format("delta")
        .load(classes_bronze_path)
        .withColumnRenamed("Código", "codigoClasse")
        .withColumnRenamed("Descricao", "descricaoClasse")
        .withColumn("codigoGrupo", F.split("Grupo", ":").getItem(0))
        .withColumn("descricaoGrupo", F.split("Grupo", ":").getItem(1))
        .withColumn("descricaoGrupo", F.trim("descricaoGrupo"))
        .withColumn("descricaoClasse", F.trim("descricaoClasse"))
        .withColumn("descricaoGrupo", F.initcap("descricaoGrupo"))
        .withColumn("descricaoClasse", F.initcap("descricaoClasse"))
        .drop("Grupo")
        .write.format("delta")
        .mode("overwrite")
        .save(classes_silver_path)
    )


# classes_landing_to_bronze()
# grupos_landing_to_bronze()
# material_landing_to_bronze()
