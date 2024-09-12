import pandas as pd
import os
import logging
from src.database.database import creating_engine

def extract_grammys():
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