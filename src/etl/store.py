from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
import pandas as pd
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = '/home/juanruizing/Workshop002_Data_Engineering/src/auth/workshop002-435619-c403e3a785a6.json'
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


def store_load(json_data):
    data = json_data[0]
    data = json.loads(data)
    data = pd.json_normalize(data)
    
    df = pd.DataFrame(data)
    # Crear un archivo CSV temporal
    temp_csv_path = '/tmp/merged_data_spotify_grammys.csv'
    df.to_csv(temp_csv_path, index=False)
    # Subir el archivo CSV a Google Drive
    upload_file(temp_csv_path)

    # Eliminar el archivo CSV temporal
    os.remove(temp_csv_path)
    return 'Data uploaded to Google Drive and Database successfully'