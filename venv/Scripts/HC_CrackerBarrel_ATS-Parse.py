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


def CB_ATS_Data():
    storage_client = storage.Client()

    for blob in storage_client.list_blobs("hc_crackerbarrel_ats", prefix="File_Upload/"):
        match = re.search("/(.+?).xlsx", str(blob))
        if match:
            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
            print("import")
            dataframe = pd.read_excel('gs://hc_crackerbarrel_ats/File_Upload/' + match.group(1) + ".xlsx",
                                      sheet_name="Sheet1")

            dataframe = dataframe.replace(to_replace=r',', value='', regex=True)

            dataframe.rename(columns={'Person : Full Name: First Last (Non-Sortable)': 'Person_FullName_FirstLast',
                                      'Application Date': 'ApplicationDate',
                                      'Job : Position Title': 'Job_PositionTitle', 'Source Channel': 'SourceChannel',
                                      'Source': 'Source', 'Source Name': 'SourceName',
                                      'Location : Store #': 'Location_Store', 'Location : City': 'Location_City',
                                      'Location : State/Province': 'Location_StateProvince',
                                      'First New Submissions': 'FirstNewSubmissions',
                                      'First Interview': 'FirstInterview', 'First Hired': 'FirstHired',
                                      'Current Application Step': 'CurrentApplicationStep',
                                      'Job : Requisition ID': 'Job_RequisitionID'}, inplace=True)

            dataframe['ApplicationDate'] = pd.to_datetime(dataframe['ApplicationDate'], errors='coerce').dt.date
            dataframe['FirstNewSubmissions'] = pd.to_datetime(dataframe['FirstNewSubmissions'], errors='coerce').dt.date
            dataframe['FirstInterview'] = pd.to_datetime(dataframe['FirstInterview'], errors='coerce').dt.date
            dataframe['FirstHired'] = pd.to_datetime(dataframe['FirstHired'], errors='coerce').dt.date

            rdata = storage_client.get_bucket('hc_crackerbarrel_ats').blob(
                'File_Resource/region_ids_dict.json').download_as_string()
            sdata = storage_client.get_bucket('hc_crackerbarrel_ats').blob(
                'File_Resource/skill_pos_ids_dict.json').download_as_string()

            regionids = json.loads(rdata)
            skillpositionid = json.loads(sdata)

            dataframe['Location_Store'] = dataframe['Location_Store'].astype(str)

            dataframe['Region'] = dataframe['Location_Store'].map(regionids)

            dataframe['Region'] = dataframe['Region'].astype(str).replace(r'\.0', '', regex=True)
            dataframe['minifiedReqID'] = dataframe['Job_RequisitionID'].replace(to_replace=r'^.*-', value='',
                                                                                regex=True, )
            dataframe['SkillPosition'] = dataframe['Job_PositionTitle']

            for key in skillpositionid:
                dataframe['SkillPosition'] = dataframe['SkillPosition'].replace(to_replace=r'^.*(?i)' + key + '.*$',
                                                                                value=skillpositionid[key], regex=True)

            dataframe['CityState'] = dataframe['Location_City'] + "-" + dataframe['Location_StateProvince']
            dataframe['Job_RequisitionID'] = dataframe['Job_RequisitionID'].astype('str')
            dataframe['Location_Store'] = dataframe['Location_Store'].astype('int64', errors='ignore')

            for p_blob in storage_client.list_blobs("hc_crackerbarrel_ats", prefix="File_Temp/"):
                p_match = re.search("/(.+?).csv", str(p_blob))
                if p_match:
                    p_dataframe = pd.read_csv("gs://hc_crackerbarrel_ats/File_Temp/" + p_match.group(1) + ".csv")

                    print(dataframe)
                    print(p_dataframe)
                    p_dataframe = p_dataframe.append(dataframe, True)
                    dataframe['Person_FullName_FirstLast'] = dataframe['Person_FullName_FirstLast'].astype(str)
                    dataframe['Job_PositionTitle'] = dataframe['Job_PositionTitle'].astype(str)
                    dataframe['Job_RequisitionID'] = dataframe['Job_RequisitionID'].astype(str)
                    p_dataframe['ApplicationDate'] = pd.to_datetime(dataframe['ApplicationDate'],
                                                                    errors='coerce').dt.date
                    p_dataframe['FirstNewSubmissions'] = pd.to_datetime(p_dataframe['FirstNewSubmissions'],
                                                                        errors='coerce').dt.date
                    p_dataframe['FirstInterview'] = pd.to_datetime(p_dataframe['FirstInterview'],
                                                                   errors='coerce').dt.date
                    p_dataframe['FirstHired'] = pd.to_datetime(p_dataframe['FirstHired'], errors='coerce').dt.date
                    print(p_dataframe)
                    p_dataframe.drop_duplicates(ignore_index=True,
                        subset=['Person_FullName_FirstLast', 'ApplicationDate',
                                'Job_PositionTitle', 'Job_RequisitionID'],
                        keep="last", inplace=True)

                    p_dataframe.to_csv("output.csv", index=False)
                    print(p_dataframe)
                    raise KeyboardInterrupt
                    found = p_match.group(1) + ".csv"
                    source_bucket = storage_client.get_bucket("hc_crackerbarrel_ats")
                    source_blob = source_bucket.blob("File_Temp/" + found)
                    destination_bucket = storage_client.get_bucket("hc_crackerbarrel_ats")
                    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Backup/" + found)
                    source_blob.delete()

                    p_dataframe.to_csv(
                        'gs://hc_crackerbarrel_ats/File_Temp/CBUpdate_' +
                        str(datetime.today().date()) + "_Merged.csv", index=False)

            found = match.group(1) + ".xlsx"
            source_bucket = storage_client.get_bucket("hc_crackerbarrel_ats")
            source_blob = source_bucket.blob("File_Upload/" + found)
            destination_bucket = storage_client.get_bucket("hc_crackerbarrel_ats")
            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
            source_blob.delete()


if __name__ == "__main__":
    start_time = time.time()
    CB_ATS_Data()
    print("--- %s seconds ---" % (time.time() - start_time))
