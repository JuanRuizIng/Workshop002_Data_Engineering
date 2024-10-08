from sqlalchemy import create_engine, Integer, Float, String, DateTime, inspect, MetaData, Table, Column, Boolean
from sqlalchemy_utils import database_exists, create_database
import os
from dotenv import load_dotenv

load_dotenv('src/auth/.env')

host = os.getenv('PG_HOST')
port = os.getenv('PG_PORT')
user = os.getenv('PG_USER')
password = os.getenv('PG_PASSWORD')
database = os.getenv('PG_DATABASE')



# Creating the connection engine from the URL made up of the environment variables
def creating_engine():
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(url)
    return engine


# Function for infer sql types
def infer_sqlalchemy_type(dtype, column_name):
    """ Map pandas dtype to SQLAlchemy's types """
    if "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        return String(500)
    elif "datetime" in dtype.name:
        return DateTime
    elif "bool" in dtype.name:
        return Boolean
    else:
        return String(500)


# Function to create table
def create_table(engine, df, table_name):
    if not inspect(engine).has_table(table_name):  # If the table doesn't exist, create it.
        metadata = MetaData()
        columns = [Column(name, infer_sqlalchemy_type(dtype, name)) for name, dtype in df.dtypes.items()]
        
        table = Table(table_name, metadata, *columns)
        table.create(engine)

        df.to_sql(table_name, engine, if_exists='replace', index=False)
    else:
        print(f'Table {table_name} already exists.')
