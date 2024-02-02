import pandas as pd
import os
from urllib.error import HTTPError

from upload_file_to_gcp import upload_to_gcp_bucket, check_if_file_exists


# pd.set_option('display.max_colwidth', None)


def get_classes_from_api():
    """
    Retrieve all classes from the API. Each request gets the first 500 records.
    The offset parameter is incremented by 500, until all the rows get fetched
    :return:
    """
    classes_endpoint = "https://compras.dados.gov.br/materiais/v1/classes.json"
    offset = 0
    count = 0
    while offset <= count:
        file_path = f'landing/classe/classes_offset_{offset}.csv'
        # if count > 0 and os.path.exists(file_path):
        #     print(f'Arquivo {file_path} já existe!')
        #     offset += 500
        #     continue
        try:
            url = f'{classes_endpoint}?offset={offset}'
            print(f'Executando: {url}')
            df = pd.read_json(url)
            if offset == 0:
                count = df['count'][0]
            df2 = pd.DataFrame.from_records(df['_embedded']['classes'])

            data = bytes(df2[['codigo', 'descricao', 'codigo_grupo']].to_csv(
                                                                index=False), encoding='ISO-8859-1')
            upload_to_gcp_bucket(data, file_path)
            print(f"Carregado offset {offset}")

            offset += 500
        except HTTPError as err:
            print(f"Erro no offset {offset}. Status Code {err.code}. Nova tentativa será realizada.")


def get_grupos_from_api():
    """
        Retrieve all groups from the API.
        :return:
        """
    file_path = 'landing/grupos/grupos.csv'
    grupos_endpoint = 'https://compras.dados.gov.br/materiais/v1/grupos.json'
    print(f'Executando: {grupos_endpoint}')
    df = pd.read_json(grupos_endpoint)
    df2 = pd.DataFrame.from_records(df['_embedded']['grupos'])

    data = bytes(df2[['codigo', 'descricao']].to_csv(index=False), encoding='ISO-8859-1')
    upload_to_gcp_bucket(data, file_path)


def get_pdms_from_api():
    """
        Retrieve all pdms from the API. Each request gets the first 500 records.
        The offset parameter is incremented by 500, until all the rows get fetched
        :return:
        """
    pdms_endpoint = "https://compras.dados.gov.br/materiais/v1/pdms.json"
    offset = 0
    count = 0
    while offset <= count:
        file_path = f'landing/pdm/pdms_offset_{offset}.csv'
        # if count > 0 and os.path.exists(file_path):
        #     print(f'Arquivo {file_path} já existe!')
        #     offset += 500
        #     continue
        try:
            url = f'{pdms_endpoint}?offset={offset}'
            print(f'Executando: {url}')
            df = pd.read_json(url)
            if offset == 0:
                count = df['count'][0]
            df2 = pd.DataFrame.from_records(df['_embedded']['pdms'])

            data = bytes(df2[['codigo', 'descricao', 'codigo_classe']].to_csv(index=False), encoding='ISO-8859-1')
            upload_to_gcp_bucket(data, file_path)
            print(f"Carregado offset {offset}")

            offset += 500
        except HTTPError as err:
            print(f"Erro no offset {offset}. Status Code {err.code}. Nova tentativa será realizada.")


def get_materiais_from_api(pdm, classe):
    """
    Retrieve all materials from the API. Each request gets the first 500 records.
    The offset parameter is incremented by 500, until all the rows get fetched
    :param grupo: filter used to reduce the number of rows
    :return:
    """
    materiais_endpoint = "https://compras.dados.gov.br/materiais/v1/materiais.json"
    offset = 0
    count = 0
    while offset <= count:
        file_path = f"landing/material_v4/materiais_classe_{classe}_pdm_{pdm}_offset_{offset}.csv"

        if check_if_file_exists(file_path):
            print(f'Arquivo já existe no bucket: {file_path}')
            break

        # # if the subsequent files already exists, skip
        # if count > 0 and os.path.exists(file_path):
        #     print(f'Arquivo {file_path} já existe!')
        #     offset += 500
        #     continue
        try:
            url = f'{materiais_endpoint}?classe={classe}&pdm={pdm}&offset={offset}'
            print(f'Executando: {url}')

            # try to get the data from API
            df = pd.read_json(url)

            # if success, get the total count
            if offset == 0:
                if df['count'].iloc[0] == 0:
                    break

                count = df['count'].iloc[0]

            # parse the json data and insert into
            df2 = pd.DataFrame.from_records(df['_embedded']['materiais'])

            data = bytes(df2[['codigo',
                              'descricao',
                              'id_grupo',
                              'id_classe',
                              'id_pdm',
                              'status',
                              'sustentavel']].to_csv(index=False), encoding='ISO-8859-1')
            upload_to_gcp_bucket(data, file_path)
            print(f"Carregado offset {offset}")

            offset += 500
        except HTTPError as err:
            print(f"Erro no offset {offset}. Status Code {err.code}. Nova tentativa será realizada.")


if __name__ == '__main__':
    # get_classes_from_api()
    # get_grupos_from_api()
    # get_pdms_from_api()
    #
    import glob

    files = glob.glob("landing/pdms/*.csv")

    list_df = []
    for f in files:
        csv = pd.read_csv(f, dtype=object)
        list_df.append(csv)
    df_pdm = pd.concat(list_df)

    for index, row in df_pdm.iterrows():
        get_materiais_from_api(pdm=row['codigo'], classe=row['codigo_classe'])

