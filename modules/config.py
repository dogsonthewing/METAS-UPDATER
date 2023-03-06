import os
import ast
from google.cloud import bigquery

credentials_path = 'G:\Meu Drive\CODES\$config\global\sa.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

def setGlobalConfig(storesConfig):
    global client
    global headers
    global table_id
    global url
    global config
    global websiteids

    config = storesConfig
    client = bigquery.Client()
    table_id = str(storesConfig['table_id'])
    headers = ast.literal_eval(storesConfig['headers'])
    url = storesConfig['url']
    websiteids = config['website_id']

    return
