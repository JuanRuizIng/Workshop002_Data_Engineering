import pandas as pd
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.database.database import creating_engine

engine = creating_engine()
query = "SELECT * FROM merged_data_spotify_grammys"

df = pd.read_sql_query(query, engine)
df.info()