import pandas as pd
import os
import logging
from src.database.database import creating_engine

def extract_spotify():
    #Read the df
    try:
        df_spotify = pd.read_csv('./data/spotify_dataset.csv')
        # Logging
        logging.info('Extracción finalizada')
        logging.debug('df', df_spotify.head(3))
    except Exception as e:
        print(f'Error: {e}')
    return df_spotify.to_json(orient='records')