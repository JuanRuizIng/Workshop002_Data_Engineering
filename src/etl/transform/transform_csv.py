import pandas as pd
import json
import os
import logging

def transform(**kwargs):
    logging.info("the kwargs are: ", kwargs)
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_task_spotify")
    json_data = json.loads(str_data)
    data = pd.json_normalize(data=json_data)
    logging.info(f"data is: {data}")
    data['region2'] = data['region']
    #function transform
    return data.to_json(orient='records')