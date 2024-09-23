# Try to change the directory to the root of the project
import os
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.database.database import creating_engine

engine = creating_engine()

# Reading the dataset
df = pd.read_csv('./src/data/the_grammy_awards.csv')

df.to_sql('grammy_awards_raw', engine, if_exists='replace', index=False)
print("Data raw loaded successfully")