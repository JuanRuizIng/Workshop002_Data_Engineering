import logging
import pandas as pd


def merge_function(data_spotify, data_grammys):
    try:
        data = pd.merge(data_spotify, data_grammys, how='left', left_on='track_name', right_on='nominee')
        data[['category', 'nominee']] = data[['category', 'nominee']].fillna('No Nominated')
        data['winner'] = data['winner'].fillna(False).astype('bool')
        data = data.dropna()
    except Exception as e:
        logging.error(f'Error: {str(e)}')

    return data