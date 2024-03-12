import pyspark.sql.functions as F
from delta import *
from utils.config import *
from utils.gcp_functions import delete_delta_data, list_files
import numpy as np


def landing_to_bronze(spark: SparkSession):
    (
        spark.read.format("parquet")
        .load(material_landing_path)
        .write.format("delta")
        .mode("overwrite")
        .save(material_bronze_path)
    )
    print(f"Loaded to bronze!")


def bronze_to_silver(spark):
    df_material = spark.read.format("delta").load(material_bronze_path)
    df_classes = spark.read.format("delta").load(classes_silver_path)
    (
        df_material.join(F.broadcast(df_classes), ["codigoClasse"], "inner")
        .select(
            "codigoItem",
            "codigoPdm",
            "nomePdm",
            "codigoClasse",
            "descricaoClasse",
            "codigoGrupo",
            "descricaoGrupo",
            "itemSuspenso",
            "itemSustentavel",
            "statusItem",
            "buscaItemCaracteristica",
        )
        .write.format("delta")
        .mode("overwrite")
        .save(material_silver_path)
    )


def silver_to_gold_material_agg(spark):

    df = spark.read.format("delta").load(material_silver_path)
    df.createOrReplaceTempView("material_silver")

    (
        spark.sql(
            """
              select codigoPdm,
              nomePdm,
              codigoClasse,
              descricaoClasse,
              codigoGrupo,
              descricaoGrupo,
              sum(case when itemSuspenso = true then 1 else 0 end) qtd_item_suspenso,
              sum(case when itemSustentavel = true then 1 else 0 end) qtd_item_sustentavel,
              sum(case when statusItem = true then 1 else 0 end) qtd_item_ativo,
              sum(case when statusItem = false then 1 else 0 end) qtd_item_inativo,
              count(*) qtd_total

              from material_silver

              group by codigoPdm, nomePdm, codigoClasse, descricaoClasse, codigoGrupo, descricaoGrupo

              """
        )
        .write.format("delta")
        .mode("overwrite")
        .save(material_pdm_gold_path)
    )


def silver_to_gold_material_char_detail(spark):
    (
        spark.read.format("delta")
        .load(material_silver_path)
        .withColumn("caracteristicas", F.explode("buscaItemCaracteristica"))
        .select(
            "codigoItem",
            "codigoPdm",
            "nomePdm",
            "codigoClasse",
            "descricaoClasse",
            "itemSuspenso",
            "itemSustentavel",
            "statusItem",
            "caracteristicas.*",
        )
        .write.format("delta")
        .mode("overwrite")
        .save(material_char_detail_path)
    )


def export_data_to_excel(spark):
    print("Exporting data...")

    (spark.read.format("delta").load(material_silver_path)).toPandas().to_excel(
        output_path + "/material_list.xlsx"
    )
    print("Material List exported to output.")

    (spark.read.format("delta").load(material_pdm_gold_path)).toPandas().to_excel(
        output_path + "/material_agg.xlsx"
    )
    print("Material Agg exported to output.")

    (spark.read.format("delta").load(material_char_detail_path)).toPandas().to_excel(
        output_path + "/material_char_detail.xlsx"
    )
    print("Material Char Detail exported to output.")
