import pandas as pd
import os
from urllib.error import HTTPError


# pd.set_option('display.max_colwidth', None)


def get_classes_from_api():
    """
    Retrieve all classes from the API. Each request gets the first 500 records.
    The offset parameter is incremented by 500, until all the rows get fetched
    :return:
    """
    offset = 0
    count = 0
    while offset <= count:
        file_path = f'landing/classes/classes_offset_{offset}.csv'
        if count > 0 and os.path.exists(file_path):
            print(f'Arquivo {file_path} já existe!')
            offset += 500
            continue
        try:
            df = pd.read_json(
                f'https://compras.dados.gov.br/materiais/v1/classes.json?offset={offset}')
            if offset == 0:
                count = df['count'][0]
            df2 = pd.DataFrame.from_records(df['_embedded']['classes'])
            df2[['codigo', 'descricao', 'codigo_grupo']].to_csv(file_path,
                                                                index=False)
            print(f"Carregado offset {offset}")

            offset += 500
        except HTTPError as err:
            if err.code == 500:
                print(f"Erro no offset {offset}. Nova tentativa será realizada.")


def get_grupos_from_api():
    """
        Retrieve all groups from the API.
        :return:
        """
    df = pd.read_json('https://compras.dados.gov.br/materiais/v1/grupos.json')
    df2 = pd.DataFrame.from_records(df['_embedded']['grupos'])
    df2[['codigo', 'descricao']].to_csv('landing/grupos/grupos.csv',
                                        index=False)


def get_pdms_from_api():
    """
        Retrieve all pdms from the API. Each request gets the first 500 records.
        The offset parameter is incremented by 500, until all the rows get fetched
        :return:
        """
    offset = 0
    count = 0
    while offset <= count:
        file_path = f'landing/pdms/pdms_offset_{offset}.csv'
        if count > 0 and os.path.exists(file_path):
            print(f'Arquivo {file_path} já existe!')
            offset += 500
            continue
        try:
            df = pd.read_json(
                f'https://compras.dados.gov.br/materiais/v1/pdms.json?offset={offset}')
            if offset == 0:
                count = df['count'][0]
            df2 = pd.DataFrame.from_records(df['_embedded']['pdms'])
            df2[['codigo', 'descricao', 'codigo_classe']].to_csv(
                file_path, index=False)
            print(f"Carregado offset {offset}")

            offset += 500
        except HTTPError as err:
            if err.code == 500:
                print(f"Erro no offset {offset}. Nova tentativa será realizada.")


def get_materiais_from_api(grupo):
    """
    Retrieve all materials from the API. Each request gets the first 500 records.
    The offset parameter is incremented by 500, until all the rows get fetched
    :param grupo: filter used to reduce the number of rows
    :return:
    """
    offset = 0
    count = 0
    while offset <= count:
        file_path = f"landing/materiais_v2/materiais_grupo_{grupo}_offset_{offset}.csv"

        # if the subsequent files already exists, skip
        if count > 0 and os.path.exists(file_path):
            print(f'Arquivo {file_path} já existe!')
            offset += 500
            continue
        try:
            # try to get the data from API
            df = pd.read_json(
                f'https://compras.dados.gov.br/materiais/v1/materiais.json'
                f'?grupo={grupo}&offset={offset}')

            # if success, get the total count
            if offset == 0:
                count = df['count'][0]

            # parse the json data and insert into
            df2 = pd.DataFrame.from_records(df['_embedded']['materiais'])
            df2[['codigo',
                 'descricao',
                 'id_grupo',
                 'id_classe',
                 'id_pdm',
                 'status',
                 'sustentavel']].to_csv(file_path,
                                        index=False)
            print(f"Carregado offset {offset}")

            offset += 500
        except HTTPError as err:
            if err.code == 500:
                print(f"Erro no offset {offset}. Nova tentativa será realizada.")


if __name__ == '__main__':
    # get_classes_from_api()
    # get_grupos_from_api()
    #
    df_grupo = pd.read_csv('landing/grupos/grupos.csv')
    for grupo in df_grupo['codigo'].to_list():
        get_materiais_from_api(grupo)
