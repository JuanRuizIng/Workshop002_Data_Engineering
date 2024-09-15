from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import sys
import json
import os
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.database.database import creating_engine, create_table

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/home/juanr/Escritorio/Workshop002_Data_Engineering/src/auth/service_account..json'
PARENT_FOLDER_ID = "1KovEf_UxmkXdqJiz6_XSmQXA6lZLtsJl"

def authenticate():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return creds

def upload_file(file_path):
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)

    file_metadata = {
        'name': "Backup of data spotify and grammys",
        'parents': [PARENT_FOLDER_ID]
    }

    media = MediaFileUpload(file_path, mimetype='text/csv')

    file = service.files().create(
        body=file_metadata,
        media_body=media
    ).execute()

def load_function(data):
    data = data[0]
    data = json.loads(data)
    data = pd.json_normalize(data)
    
    df = pd.DataFrame(data)
    # Crear un archivo CSV temporal
    temp_csv_path = '/tmp/merged_data_spotify_grammys.csv'
    df.to_csv(temp_csv_path, index=False)
    # Subir el archivo CSV a Google Drive
    upload_file(temp_csv_path)

    engine = creating_engine()
    create_table(engine, df, 'merged_data_spotify_grammys')

    # Eliminar el archivo CSV temporal
    os.remove(temp_csv_path)
    return 'Data uploaded to Google Drive and Database successfully'