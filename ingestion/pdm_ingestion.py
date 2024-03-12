import socket

from utils.config import (
    classes_endpoint,
    pdms_endpoint,
    grupos_endpoint,
    material_endpoint,
    material_landing_path,
)

socket.setdefaulttimeout(300)
import pandas as pd
from urllib.error import HTTPError

from utils.gcp_functions import upload_to_gcp_bucket


def get_pdms_from_api():
    offset = 500
    while True:
        file_path = f"landing/pdm/pdms_offset_{offset}.csv"

        try:
            url = f"{pdms_endpoint}?offset={offset}"
            print(f"Running: {url}")

            df = pd.read_csv(url, header=0, encoding="ISO-8859-1")
            data = bytes(df.to_csv(index=False), encoding="ISO-8859-1")

            upload_to_gcp_bucket(data, file_path, "text/csv")
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
