import os

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from config.env import doc_url
from utils import access_token, flatten_json
from commons import application_name, form_name, max_count,output_dir,etq

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

output_file = os.path.join(output_dir,etq)

def fetch_data(count):
    try:
        headers = {
            'Authorization': "",
            'Content-Type': 'application/json'
        }
        url = f'{doc_url}/{application_name}/{form_name}/{count}'
        response = requests.get(url, headers=headers)

        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

        json_data_list = response.json().get('Document', [])
        flattened_data_list = [flatten_json(item) for item in json_data_list]

        return flattened_data_list
    except requests.exceptions.RequestException as e:
        print(f'Error processing API {count}: {e}')
        return None


def process_api(count):
    etq_doc = fetch_data(count)
    if etq_doc is not None:
        print(f'Processed API {count}')
        return etq_doc

all_data = []  # List to store all fetched data
max_count = int(max_count)  # Convert max_count to an integer

# Using ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=100) as executor:  # Adjust max_workers based on your needs
    results = list(executor.map(process_api, range(1, max_count + 1)))

# Flatten the results (list of lists) into a single list
all_data = [item for sublist in results if sublist is not None for item in sublist]

df = pd.DataFrame(all_data)
df.to_csv(output_file, index=False)
