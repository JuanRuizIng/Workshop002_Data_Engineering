import pandas as pd
import logging
from src.database.database import creating_engine

def extract_db():
    #Read the df
    try:
        #Read the environment variables
        engine = creating_engine()
        query = 'SELECT * FROM grammy_awards_raw'
        df_grammy = pd.read_sql(query, engine)
        # Logging
        logging.info('Extracción finalizada')
        logging.debug('df', df_grammy.head(3))
    except Exception as e:
        print(f'Error: {e}')
    return df_grammy.to_json(orient='records')

def transform_db(json_data):
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))
    data = pd.json_normalize(data=json_data)
    #function transform
    #return json_data
    pass