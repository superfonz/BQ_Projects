import gcsfs
import os
import re
import google.auth
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'

def AISImport():
    client = storage.Client()

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    for blob in client.list_blobs("hc_ais_ats", prefix="File_Upload/"):
        f = re.search("/(.+?).csv", str(blob))
        if f:
            dataframe = pd.read_csv('gs://hc_ais_ats/File_Upload/' + f.group(1) + ".csv", header=0, dtype=str)

            for col in dataframe.columns:
                sub = re.sub(" ","_",col)
                dataframe.rename(columns={col: sub}, inplace=True)

            dataframe['Submittal_Date'] = pd.to_datetime(dataframe['Submittal_Date'], errors='coerce').dt.date
            dataframe['Date_Opened'] = pd.to_datetime(dataframe['Date_Opened'], errors='coerce').dt.date
            dataframe['Status_Date'] = pd.to_datetime(dataframe['Status_Date'], errors='coerce').dt.date
            dataframe = dataframe.astype({'Days_Open': int,
                                          'No_of_Interviews': int,
                                          'No_of_Offers_Accepted': int,
                                          'No_of_offers_Declined': int,
                                          'No_of_Phone_Screens': int,
                                          'No_of_Submittals': int})

            pandas_gbq.to_gbq(dataframe, "AIS.AIS_ATS", project_id='hireclix', if_exists='replace',
                              table_schema=[
                                  {'name': 'Submittal_Date', 'type': 'DATE'},
                                  {'name': 'Date_Opened', 'type': 'DATE'},
                                  {'name': 'Status_Date', 'type': 'DATE'}
                              ])
            dataframe.to_csv('gs://hc_ais_ats/File_Backup/' + f.group(1) + "_" +
                             str(datetime.today().date()) + ".csv", index=False)

            found = f.group(1) + ".csv"
            source_bucket = client.get_bucket("hc_ais_ats")
            source_blob = source_bucket.blob("File_Upload/" + found)
            destination_bucket = client.get_bucket("hc_ais_ats")
            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
            source_blob.delete()


AISImport()