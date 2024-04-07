import socket

socket.setdefaulttimeout(300)


from utils.config import classes_endpoint, classes_landing_path
import pandas as pd
from urllib.error import HTTPError


def get_classes_from_api():
    """
    Retrieve all classes from the API. Each request gets the first 500 records.
    The offset parameter is incremented by 500, until all the rows get fetched
    :return:
    """
    offset = 0
    while True:
        file_path = f"{classes_landing_path}/classes_offset_{offset}.csv"

        try:
            url = f"{classes_endpoint}?offset={offset}"
            print(f"Running: {url}")

            df = pd.read_csv(url, header=0, encoding="ISO-8859-1")
            df.to_csv(file_path, index=False, encoding="ISO-8859-1")

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
