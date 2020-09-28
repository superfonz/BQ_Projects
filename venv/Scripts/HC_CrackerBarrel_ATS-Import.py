import json
import os
import re
import time
from datetime import datetime, timedelta

import gcsfs
import google.auth
import numpy as np
import pandas as pd
import pandas_gbq
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def CB_ATS_Data_import():
    storage_client = storage.Client()

    for blob in storage_client.list_blobs("hc_crackerbarrel_ats", prefix="File_Temp/"):
        match = re.search("/(.+?).csv", str(blob))
        if match:
            dataframe = pd.read_csv('gs://hc_crackerbarrel_ats/File_Upload/' + match.group(1) + ".csv")
            pandas_gbq.to_gbq(dataframe, 'crackerbarrel.crackerbarrel_ats_TEST', project_id='hireclix',
                              if_exists='replace',
                              table_schema=[{'name': 'Person_FullName_FirstLast', 'type': 'STRING'},
                                            {'name': 'ApplicationDate', 'type': 'DATE'},
                                            {'name': 'Job_PositionTitle', 'type': 'STRING'},
                                            {'name': 'SourceChannel', 'type': 'STRING'},
                                            {'name': 'Source', 'type': 'STRING'},
                                            {'name': 'SourceName', 'type': 'STRING'},
                                            {'name': 'Location_Store', 'type': 'INTEGER'},
                                            {'name': 'Location_City', 'type': 'STRING'},
                                            {'name': 'Location_StateProvince', 'type': 'STRING'},
                                            {'name': 'FirstNewSubmissions', 'type': 'DATE'},
                                            {'name': 'FirstInterview', 'type': 'DATE'},
                                            {'name': 'FirstHired', 'type': 'DATE'},
                                            {'name': 'CurrentApplicationStep', 'type': 'STRING'},
                                            {'name': 'Job_RequisitionID', 'type': 'STRING'},
                                            {'name': 'Region', 'type': 'STRING'},
                                            {'name': 'minifiedReqID', 'type': 'STRING'},
                                            {'name': 'SkillPosition', 'type': 'STRING'},
                                            {'name': 'CityState', 'type': 'STRING'}])


if __name__ == "__main__":
    start_time = time.time()
    CB_ATS_Data_import()
    print("--- %s seconds ---" % (time.time() - start_time))
