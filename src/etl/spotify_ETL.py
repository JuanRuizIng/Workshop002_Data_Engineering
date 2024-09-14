import pandas as pd
import logging
from src.database.database import creating_engine

def extract_csv():
    #Read the df
    try:
        df_spotify = pd.read_csv('./data/spotify_dataset.csv')
        logging.info('Extracción finalizada')
        logging.debug('df', df_spotify.head(3))
    except Exception as e:
        print(f'Error: {e}')
    return df_spotify.to_json(orient='records')

def transform_csv(json_data):
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))
    data = pd.json_normalize(data=json_data)
    logging.info(f"data is: {data}")
    #function transform
    #return json_data
    pass