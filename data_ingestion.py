import socket

from global_variables import classes_endpoint, pdms_endpoint, grupos_endpoint, material_endpoint, material_landing_path

socket.setdefaulttimeout(300)
import pandas as pd
from urllib.error import HTTPError
import requests
import pyspark.sql.functions as F

from gcp_functions import upload_to_gcp_bucket



def get_classes_from_api():
    """
    Retrieve all classes from the API. Each request gets the first 500 records.
    The offset parameter is incremented by 500, until all the rows get fetched
    :return:
    """
    offset = 0
    while True:
        file_path = f'landing/classes/classes_offset_{offset}.csv'

        try:
            url = f'{classes_endpoint}?offset={offset}'
            print(f'Running: {url}')

            df = pd.read_csv(url, header=0, encoding='ISO-8859-1')
            data = bytes(df.to_csv(index=False), encoding='ISO-8859-1')

            upload_to_gcp_bucket(data, file_path, 'text/csv')
            print(f"Offset loaded {offset}")

            count = len(df)
            print(f"Number of rows: {count}")
            if count < 500:
                break

            offset += 500
        except HTTPError as err:
            print(f"Error at offset {offset}. Status Code {err.code}. Retrying soon.")
        except TimeoutError as err:
            print("Request timed out. Retrying soon.")


def get_grupos_from_api():
    """
        Retrieve all groups from the API.
        :return:
        """
    file_path = 'landing/grupos/grupos.csv'
    print(f'Running: {grupos_endpoint}')
    df = pd.read_csv(grupos_endpoint, header=0, encoding='ISO-8859-1')

    data = bytes(df.to_csv(index=False), encoding='ISO-8859-1')
    upload_to_gcp_bucket(data, file_path, 'text/csv')


# def get_materiais_from_api_v2(spark, pdm):

#     # Fetch JSON data from the website
#     response = requests.get(f"https://cnbs.estaleiro.serpro.gov.br/cnbs-api/material/v1/materialCaracteristcaValorporPDM?codigo_pdm={pdm}")
#     json_data_str = response.text

#     # Read JSON data from the string
#     json_data = spark.read.json(spark.sparkContext.parallelize([json_data_str]))

#     df_count = json_data.count()    

#     if df_count == 0:
#         return
    
#     print(f"JSON loaded. Count: {df_count}")

#     # Define GCS parameters
#     gcs_bucket = "compras-bucket"
#     gcs_path = f"gs://{gcs_bucket}/landing/material_v5/pdm_{pdm}"

#     json_data.write.format('parquet').mode("overwrite").save(gcs_path)
#     print(f'File {gcs_path} saved in the bucket.')

def get_material_from_api(pdm):

    # Fetch JSON data from the website
    response = requests.get(f"{material_endpoint}/materialCaracteristcaValorporPDM?codigo_pdm={pdm}")
    json_data_str = response.text

    filepath = f"landing/material/material_pdm_{pdm}"

    upload_to_gcp_bucket(data=json_data_str, destination_blob_name=filepath, file_type= 'text/json')

    print(f'File {filepath} saved in the bucket.')

def get_pdms_from_api():
    offset = 500
    while True:
        file_path = f'landing/pdm/pdms_offset_{offset}.csv'

        try:
            url = f'{pdms_endpoint}?offset={offset}'
            print(f'Running: {url}')

            df = pd.read_csv(url, header=0, encoding='ISO-8859-1')
            data = bytes(df.to_csv(index=False), encoding='ISO-8859-1')

            upload_to_gcp_bucket(data, file_path, 'text/csv')
            print(f"Offset loaded {offset}")

            count = len(df)
            print(f"Number of rows: {count}")
            if count < 500:
                break

            offset += 500
        except HTTPError as err:
            print(f"Error at offset {offset}. Status Code {err.code}. Retrying soon.")
        except TimeoutError as err:
            print("Request timed out. Retrying soon.")


def get_all_materials_from_api(spark):
    """
        Retrieve all materials using the pdms code previously loaded
    """    

    df_pdms = (spark
               .read
               .format("csv")
               .option("header", True)
               .option("inferSchema", False)
               .load(material_landing_path)
               .drop_duplicates())

    pdms = list(df_pdms.select('codigo').toPandas()['codigo'])

    for pdm in pdms:
        get_material_from_api(pdm)