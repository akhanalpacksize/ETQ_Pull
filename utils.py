from base64 import b64encode

import requests
from config.env import auth_url, client_id, client_secret,LOCAL_CLIENT_SECRET,LOCAL_CLIENT_ID,CLIENT_ID,CLIENT_SECRET,GRANT_TYPE,AUTH_URL


def flatten_json(json_data, parent_key='', separator='_'):
    flattened = {}

    # Additional columns
    flattened['applicationName'] = json_data.get('applicationName', '')
    flattened['formName'] = json_data.get('formName', '')
    flattened['documentId'] = json_data.get('documentId', '')
    flattened['phase'] = json_data.get('phase', '')

    fields = json_data.get('Fields', [])
    for field in fields:
        field_name = field.get('fieldName', '')
        values = field.get('Values', [])

        if not values:
            # If values are empty, add the field with NaN
            flattened[field_name] = float('nan')
        else:
            # Use only field_name as the column name
            flattened[field_name] = values[0]  # Use the first value for simplicity, you might want to adjust this

    return flattened


def get_ETQ_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    }
    payload = {
        'grant_type': 'client_credentials'
    }
    try:
        response = requests.post(auth_url, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token
    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")


access_token = get_ETQ_token()


def get_access_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
    }

    payload = {
        "grant_type": GRANT_TYPE,
    }

    try:
        response = requests.post(AUTH_URL, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")


# get Local instance access token
def get_local_access_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{LOCAL_CLIENT_ID}:{LOCAL_CLIENT_SECRET}'.encode()).decode()
    }

    payload = {
        "grant_type": GRANT_TYPE
    }

    try:
        response = requests.post(AUTH_URL, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")
