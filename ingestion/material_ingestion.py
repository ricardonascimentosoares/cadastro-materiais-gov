import socket

socket.setdefaulttimeout(300)


from utils.gcp_functions import check_if_file_exists, upload_to_gcp_bucket
from utils.config import *
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


# @udf
# def get_materiais_from_api_v2(pdm):

#     file_path = f"{material_landing_path}/{pdm}"

#     if check_if_file_exists(f"landing/material_parquet/{pdm}"):
#         print(f"File {file_path} already exists!")
#         return True

#     try:
#         df = pd.read_json(f"https://cnbs.estaleiro.serpro.gov.br/cnbs-api/material/v1/materialCaracteristcaValorporPDM?codigo_pdm={pdm}")
#         df.to_parquet(file_path, index=False)
#         print(f"Materials from PDM {pdm} saved.")
#     except:
#         print(f"Erro ao captura os dados do pdm {pdm}")
#         return False

#     return True


def get_all_materials_from_api(spark):
    """
    Retrieve all materials using the pdms code previously loaded
    """

    df_pdms_codigo = (
        spark.read.format("delta")
        .load(pdm_silver_path)
        .select("codigoPdm")
        .where(
            "codigoClasse in (6505,6508,6509,6510,6515,6520,6525,6530,6532,6540,6545,6555)"
        )
    )
    pdms = list(df_pdms_codigo.toPandas()["codigoPdm"])

    df_final = pd.DataFrame()
    for pdm in pdms:
        filepath = f"{material_landing_path}/{pdm}"
        url = f"https://cnbs.estaleiro.serpro.gov.br/cnbs-api/material/v1/materialCaracteristcaValorporPDM?codigo_pdm={pdm}"
        try:
            df = pd.read_json(url)
            if len(df) == 0:
                print(f"PDM {pdm} empty")
                continue
            df_final = pd.concat([df_final, df], ignore_index=True, sort=False)
            print(f"Materials from PDM {pdm} saved.")
        except Exception:
            print(f"Erro. {url}")

    df_final.to_parquet(filepath, index=False)
    print("Data saved into landing zone with success")
