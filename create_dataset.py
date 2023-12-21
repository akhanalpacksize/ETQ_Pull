import csv
import json
import os
import requests
import logging

from utils import get_access_token, get_local_access_token
from commons import etq,output_dir,DATASET_OWNER_ID,DATA_SET_OWNER_NAME
from config.env import BASE_URL
from logger_config import setup_logging

# Setup logging
setup_logging(module_name="create_dataset")

logger = logging.getLogger(__name__)


def get_csv_file_headers(file_path):
    """Returns the headers of a CSV file as a list."""
    with open(file_path, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        header = next(csv_reader)
        return header


# GENERATE SCHEMA COLUMNS FOR DOMO DATASET
def generate_schema_columns(header):
    """Returns a list of schema columns based on the headers of a CSV file."""
    schema_column = []
    for elem in header:
        schema_column.append({
            "type": "string",
            "name": elem
        })
    return schema_column


def create_domo_dataset(name, description, schema_column, owner_id, owner_name, token):
    payload = {
        "name": name,
        "description": description,
        "rows": 0,
        "columns": len(schema_column),
        "schema": {
            "columns": schema_column
        },
        "owner": {
            "id": owner_id,
            "name": owner_name
        },
    }

    header = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(f'{BASE_URL}/datasets', headers=header, json=payload)

    if response.ok:
        json_res = json.loads(response.text)
        data_set_id = json_res.get('id')
        logger.info(f"Created dataset '{name}' with ID {data_set_id}")
    else:
        logger.error(f"Failed to create dataset '{name}' with status code {response.status_code}")
        response.raise_for_status()


def generate_dataset():
    # GETS ACCESS TOKEN FOR DOMO TO CREATE DATASET
    access_token = get_access_token()
    # access_token = get_local_access_token()

    # PATH TO CSV FILE
    csv_file_path = os.path.join(os.getcwd(), output_dir, etq)

    # GETS HEADER FOR CSV FILE
    headers = get_csv_file_headers(csv_file_path)

    # GETS HEADER FOR DOMO DATASET
    schema_columns = generate_schema_columns(headers)

    # GIVES DATASET NAME AND EXTENSION
    dataset_name = "ETQ TASKMANAGEMENT - TASK FORM"
    dataset_description = "Contains ETQ data App Name: ETQ_TASKMANAGEMENT, Form Name: TASK_FORM"

    try:
        # CREATE DATASET FOR DOMO
        create_domo_dataset(dataset_name, dataset_description, schema_columns,
                            DATASET_OWNER_ID, DATA_SET_OWNER_NAME, access_token)
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred while creating dataset '{dataset_name}': {e}")


generate_dataset()
