import pandas as pd
import logging
import sys
import os
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.etl.transformation import transform_spotify, transform_grammys
from src.etl.merge import merge_function
from src.etl.load import load_function

load_dotenv('src/auth/.env')

host = os.getenv('PG_HOST')
port = os.getenv('PG_PORT')
user = os.getenv('PG_USER')
password = os.getenv('PG_PASSWORD')
database = os.getenv('PG_DATABASE')

url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(url)


def extract_csv():
    #Read the df
    try:
        df_spotify = pd.read_csv('./src/data/spotify_dataset.csv')
        logging.info('Extracción finalizada')
        logging.debug('df', df_spotify.head(2))
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    return df_spotify.to_json(orient='records')


def extract_db():
    #Read the df
    try:
        df_grammy = pd.read_sql("SELECT * FROM grammy_awards_raw", engine)
        # Logging
        logging.info('Extracción finalizada')
        logging.debug('df', df_grammy.head(2))
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    return df_grammy.to_json(orient='records')


def transform_db(json_data):
    try:
        print("data coming from extract:", json_data)
        print("data type is: ", type(json_data))
        json_string = json_data[0]
        json_data = json.loads(json_string)
        print(f"data after extraction is: {json_data}")
        data = pd.json_normalize(data=json_data)
        data = transform_grammys(data)
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    return data.to_json(orient='records')


def transform_csv(json_data):
    try:
        print("data coming from extract:", json_data)
        print("data type is: ", type(json_data))
        json_string = json_data[0]
        json_data = json.loads(json_string)
        print(f"data after extraction is: {json_data}")
        data = pd.json_normalize(data=json_data)
        logging.info(f"data is: {data}")
        data = transform_spotify(data)
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    return data.to_json(orient='records')


def merge(json_data_spotify, json_data_grammys):
    try:
        print("data database coming from extract:", json_data_grammys)
        print("data csv coming from extract:", json_data_spotify)
        print("data database type is: ", type(json_data_grammys))
        print("data csv type is: ", type(json_data_spotify))

        json_string_grammys = json_data_grammys[0]
        json_string_spotify = json_data_spotify[0]
        json_data_grammys = json.loads(json_string_grammys)
        json_data_spotify = json.loads(json_string_spotify)
        data_grammys = pd.json_normalize(data=json_data_grammys)
        data_spotify = pd.json_normalize(data=json_data_spotify)

        logging.info(f"data database after extrac is: {data_grammys}")
        logging.info(f"data csv after extract is: {data_spotify}")
        logging.info(f"data database is: {data_grammys}")
        logging.info(f"data csv is: {data_spotify}")

        data = merge_function(data_spotify, data_grammys)

        logging.info(f"data merged is: {data}")
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    return data.to_json(orient='records')


def load(json_data):
    try:
        data = pd.json_normalize(data=json_data)
        logging.info(f"data after extract is: {data}")
        load_function(data)
    except Exception as e:
        logging.error(f'Error: {str(e)}')
    return 'Data loaded successfully'
