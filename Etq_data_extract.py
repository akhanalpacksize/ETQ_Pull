import os
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from config.env import doc_url
from utils import access_token, flatten_json
from commons import output_dir, etq


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

output_file = os.path.join(output_dir, etq)