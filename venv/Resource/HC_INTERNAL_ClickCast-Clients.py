import gcsfs
import os
import re
import google.auth
import requests
import json

from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def hc_internal_clickcast_clients():


    # <---Authenticate and initialize--->
    # Data Store API Client
    storage_client = storage.Client()

    # BigQuery API Client
    bigquery_client = bigquery.Client()

    #  BigQuery API Extension Client
    biqquery_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
    # <--------------------------------->

    with open('/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/ClickCastToken.json') as token:
        headers = json.load(token)

    payload = {
        'period': '1_DAYS_AGO'
    }

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    clientstats = requests.get("https://api.clickcast.cloud/clickcast/api/employers/stats", headers=headers,
                               params=payload)

    json_object = json.loads(clientstats.text)

    try:
        results = json.dumps(json_object['results'], indent=2)
        dataframe = pd.read_json(results)
        dataframe = dataframe.astype({'cpa': float,
                                      'cpa_paid': float,
                                      'cpc': float,
                                      'cpc_paid': float,
                                      'cr': float,
                                      'cr_paid': float,
                                      'cr_organic': float,
                                      'cpas': float,
                                      'spend': float})
    except KeyError:
        print("error Illegal Param")

    pandas_gbq.to_gbq(dataframe, 'ClickCast.clients', project_id='hireclix',
                      if_exists='replace',
                      table_schema=[{'name': 'employer_id', 'type': 'STRING'},
                                    {'name': 'employer_name', 'type': 'STRING'},
                                    {'name': 'start_date', 'type': 'DATE'},
                                    {'name': 'end_date', 'type': 'DATE'},
                                    {'name': 'applies', 'type': 'INTEGER'},
                                    {'name': 'applies_organic', 'type': 'INTEGER'},
                                    {'name': 'applies_paid', 'type': 'INTEGER'},
                                    {'name': 'applies_unpaid', 'type': 'INTEGER'},
                                    {'name': 'apply_starts', 'type': 'INTEGER'},
                                    {'name': 'clicks', 'type': 'INTEGER'},
                                    {'name': 'clicks_organic', 'type': 'INTEGER'},
                                    {'name': 'clicks_paid', 'type': 'INTEGER'},
                                    {'name': 'clicks_unpaid', 'type': 'INTEGER'},
                                    {'name': 'cpa', 'type': 'FLOAT'},
                                    {'name': 'cpa_paid', 'type': 'FLOAT'},
                                    {'name': 'cpc', 'type': 'FLOAT'},
                                    {'name': 'cpc_paid', 'type': 'FLOAT'},
                                    {'name': 'cr', 'type': 'FLOAT'},
                                    {'name': 'cr_paid', 'type': 'FLOAT'},
                                    {'name': 'cr_organic', 'type': 'FLOAT'},
                                    {'name': 'cpas', 'type': 'FLOAT'},
                                    {'name': 'currency', 'type': 'STRING'},
                                    {'name': 'device', 'type': 'STRING'},
                                    {'name': 'jobs', 'type': 'INTEGER'},
                                    {'name': 'jobs_sponsored', 'type': 'INTEGER'},
                                    {'name': 'jobs_exported', 'type': 'INTEGER'},
                                    {'name': 'spend', 'type': 'FLOAT'},
                                    {'name': 'publisher_revenue', 'type': 'INTEGER'},
                                    {'name': 'projected_spend', 'type': 'INTEGER'},
                                    {'name': 'revenue', 'type': 'INTEGER'},
                                    {'name': 'budget', 'type': 'INTEGER'},
                                    {'name': 'impressions', 'type': 'INTEGER'},
                                    {'name': 'gross_margin', 'type': 'INTEGER'}])
    dataframe.to_csv(
        'gs://hc_clickcast/client/clientdata_' + str(datetime.today().date()) + ".csv", index=False)


hc_internal_clickcast_clients()