import os
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from config.env import doc_url
from utils import access_token, flatten_json, get_ETQ_token
from commons import output_dir, etq, etq_json
from datetime import datetime
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

output_file = os.path.join(output_dir, etq)
output_file_json = os.path.join(output_dir, etq_json)
#
# # Example: Assuming you read application_name and form_name from a CSV file
etq_p = pd.read_csv('etq_forms_by_applications.csv')
application_name = etq_p['application_name']
form_name = etq_p['form_name']

def fetch_data(count, app_name, f_name):
    try:
        headers = {
            'Authorization': get_ETQ_token(),
            'Content-Type': 'application/json'
        }
        url = f'{doc_url}/{app_name}/{f_name}/{count}'
        timestamp = datetime.utcnow()
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

        json_data = response.json().get('Document')
        return {'json_data': json_data, 'app_name': app_name, 'form_name': f_name, 'doc_id': count,'success':True, "timestamp": timestamp}
    except Exception as e:

        print(f'Error processing API {count} for {app_name}/{f_name}: {e}')
        return {'json_data': f"{e.response.text}", 'app_name': app_name, 'form_name': f_name, 'doc_id': count, 'success':False, "timestamp":timestamp}

# fetch_data(1, "ASPECTS", "ASPECTS_ASPECT_DOCUMENT")

def process_api(app_name, f_name):
    all_data_for_api = []  # List to store all fetched data for a specific app_name and f_name
    valid_response_count = 0
    # Iterate over the count for the given app_name and f_name until we get 10 valid responses or reach max_count
    counter=0
    for count in range(1,2):
        etq_doc = fetch_data(count, app_name, f_name)
        counter+=1

        if etq_doc.get('success'):
            all_data_for_api.append(etq_doc)
            valid_response_count += 1
            if valid_response_count == 10:
                break
        else:
            if counter == 1 and (app_name + "->" + f_name not in [app.get('app_name') + "->" + app.get('form_name') for app in all_data_for_api]):
                print(f"Error processing API {count} for {app_name}/{f_name}")
                all_data_for_api.append(etq_doc)
    if len(all_data_for_api) > 0:
        df = pd.DataFrame(all_data_for_api)
        if not os.path.exists(output_file):
            df.to_csv(output_file, index=False, header=True,
                      columns=["json_data", "app_name", "form_name", "doc_id", "success", "timestamp"])
        else:
            df.to_csv(output_file,mode="a", index=False, header=False,
                      columns=["json_data", "app_name", "form_name", "doc_id", "success", "timestamp"])
        if not os.path.exists(output_file_json):
            df.to_json(output_file_json, orient='records', lines=True)
        else:
            df.to_json(output_file_json, orient='records', lines=True, mode='a')

    return all_data_for_api


all_data = []  # List to store all fetched data
# max_count = 1  # Initial upper limit for count

def process_app_data():
    with ThreadPoolExecutor(max_workers=100) as executor:
        args_list = [(app_name, f_name) for app_name, f_name in zip(application_name, form_name)]
        executor.map(lambda args: process_api(*args), args_list)
process_app_data()

def test_csv():
    df = pd.read_csv(output_file)
    counter=0
    # data= df[(df['app_name'] == "CORRACT") & (df['form_name'] == "CORRACT_EXTENSION_REASSIGNMENT_REQUEST")]
    # print(data)
    for app_name, f_name in zip(application_name, form_name):
        counter+=1
        count = len(df[(df['app_name'] == app_name) & (df['form_name'] == f_name)])
        print("{}. {} - {} => count {}".format(counter, app_name, f_name, count))
    true_case = df['success'].value_counts()
    false_case = df['success'].value_counts()
    print(true_case, false_case)

test_csv()



# Iterate until all combinations have at least 10 valid responses or max_count is reached
# while max_count <= 5000:
# with ThreadPoolExecutor(max_workers=100) as executor:
#         args_list = [(app_name, f_name) for app_name, f_name in zip(application_name, form_name)]
#         results = list(executor.map(lambda args: process_api(*args), args_list))
#         data = [item for sublist in results for item in sublist]

    # if not data:
    #     # No data fetched, break out of the loop
    #     break
    #
    # # Check if all combinations have at least 10 valid responses
    # if len(data) == len(application_name) * len(form_name) * 10:
    #     break