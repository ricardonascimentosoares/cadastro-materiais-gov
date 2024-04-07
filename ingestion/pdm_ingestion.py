import socket

socket.setdefaulttimeout(300)


from utils.config import pdms_endpoint, pdm_landing_path

import pandas as pd
from urllib.error import HTTPError

from utils.gcp_functions import upload_to_gcp_bucket


def get_pdms_from_api():
    offset = 0
    while True:
        file_path = f"{pdm_landing_path}/pdms_offset_{offset}.csv"

        try:
            url = f"{pdms_endpoint}?offset={offset}"
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


# if __name__ == '__main__':
#     get_pdms_from_api()
