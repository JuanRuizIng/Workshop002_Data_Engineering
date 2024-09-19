import sys
import json
import os
import pandas as pd
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.database.database import creating_engine, create_table

def load_function(json_data):
    data = json_data[0]
    data = json.loads(data)
    data = pd.json_normalize(data)
    
    df = pd.DataFrame(data)

    engine = creating_engine()
    create_table(engine, df, 'merged_data_spotify_grammys')

    return data