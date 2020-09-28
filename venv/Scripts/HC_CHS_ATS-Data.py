from __future__ import absolute_import
import gcsfs
import os
import re
import google.auth
from datetime import datetime, timedelta
import time
import io

import pandas as pd
import pandas_gbq
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1beta1, storage
import google.auth
from threading import Timer
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openpyxl as opxl
import json

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def CHS_ATS():
    client = storage.Client()  # Storage
    bqclient = bigquery.Client()  # Readable BQ
    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient()  # writable to BQ

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    credraw = client.get_bucket('hc_tokens_scripts').blob(
        'Tokens/hireclix-googlesheets.json').download_as_string()

    credjson = json.loads(credraw)

    cred = ServiceAccountCredentials.from_json_keyfile_dict(credjson, scope)

    gclient = gspread.authorize(cred)

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    # ----- Input method needs to change
    for blob in client.list_blobs("hc_chs_ats", prefix="File_Upload/"):
        f = re.search("/(.+?).csv", str(blob))

        if f:
                dataframe = pd.read_csv('gs://hc_chs_ats/File_Upload/' + f.group(1) + ".csv", header=0)

                sheet = gclient.open_by_key('1sIux7eUPi5q3Aeg4hM-PzV2Kr9jIoifqzZhz818SVaE').worksheet('Location Key')
                values = sheet.get_all_values()
                df = pd.DataFrame(values, columns=values[0])
                df.drop([0], inplace=True)
                df.drop(['Company', 'Entity', 'Entity_Desc', 'Record Count'], axis=1, inplace=True)
                df.set_index('Company_Code', inplace=True)
                locationkeys = df.to_dict()

                query_string = """
                            SELECT 
                            * 
                            FROM 
                            `hireclix.chs.ats_master`
                            """
                try:
                    print("Retrieving data")
                    bgdata = (
                        bqclient.query(query_string)
                            .result()
                            .to_dataframe(bqstorage_client=bqstorageclient)
                    )
                    dataframe = bgdata.append(dataframe, True)
                    print("dedup")
                    dataframe['Date_Applied'] = pd.to_datetime(dataframe['Date_Applied'], errors='coerce').dt.date
                    dataframe['Step_Date'] = pd.to_datetime(dataframe['Step_Date'], errors='coerce').dt.date
                    dataframe['Recruiting_Hire_Date'] = pd.to_datetime(dataframe['Recruiting_Hire_Date'],
                                                                       errors='coerce').dt.date
                    dataframe['Start_Date'] = pd.to_datetime(dataframe['Start_Date'], errors='coerce').dt.date
                    dataframe['First_Published_Date'] = pd.to_datetime(dataframe['First_Published_Date'],
                                                                       errors='coerce').dt.date
                    dataframe['Source_Job_Code'] = dataframe['Source_Job_Code'].astype(str).replace(r'\.0', '', regex=True)
                    dataframe['Company_Code'] = dataframe['Company_Code'].astype(str).replace(r'\.0', '', regex=True)

                    dataframe['Average_Days_Between_Publish_Hire_Dates'] = dataframe[
                        'Average_Days_Between_Publish_Hire_Dates'].astype(str).replace(r'\.0', '', regex=True)

                    dataframe.drop_duplicates(keep="last", ignore_index=True, inplace=True,
                                              subset=['Requisition_Number', 'Opportunity_Title',
                                                      'Company', 'Source_Job_Code', 'Job_Title',
                                                      'Recruiter', 'Date_Applied', 'Source', 'Step', 'Step_Date',
                                                      'Candidate', 'First_Published_Date'])

                    dataframe['Company_Code'] = dataframe['Company_Code'].astype(str).replace(r'\.0', '', regex=True)

                    dataframe['Hospital_System'] = dataframe['Company_Code'].map(locationkeys['Hospital System'])
                    dataframe['Hospital_System'].fillna('Other', inplace=True)

                except exceptions.NotFound:
                    print("Big query Table not Found, Creating New one")

                print("fixing GCS")
                dataframe.to_csv(
                    'gs://hc_chs_ats/File_Backup/' + f.group(1) + "_" +
                    str(datetime.today().date()) + "_Merged.csv", index=False)
                found = f.group(1) + ".csv"
                source_bucket = client.get_bucket("hc_chs_ats")
                source_blob = source_bucket.blob("File_Upload/" + found)
                destination_bucket = client.get_bucket("hc_chs_ats")
                blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
                source_blob.delete()

                pandas_gbq.to_gbq(dataframe, 'chs.ats_master', project_id='hireclix',
                                  if_exists='replace',
                                  table_schema=[{'name': 'Requisition_Number', 'type': 'STRING'},
                                                {'name': 'Opportunity_Title', 'type': 'STRING'},
                                                {'name': 'Opportunity_Status', 'type': 'STRING'},
                                                {'name': 'Featured', 'type': 'STRING'},
                                                {'name': 'Company_Code', 'type': 'STRING'},
                                                {'name': 'Company', 'type': 'STRING'},
                                                {'name': 'Entity', 'type': 'STRING'},
                                                {'name': 'Entity_Desc', 'type': 'STRING'},
                                                {'name': 'Source_Job_Code', 'type': 'STRING'},
                                                {'name': 'Job_Title', 'type': 'STRING'},
                                                {'name': 'FullTime_Or_PartTime', 'type': 'STRING'},
                                                {'name': 'Salary_Or_Hourly', 'type': 'STRING'},
                                                {'name': 'Recruiter', 'type': 'STRING'},
                                                {'name': 'Location_Name', 'type': 'STRING'},
                                                {'name': 'Date_Applied', 'type': 'DATE'},
                                                {'name': 'Source', 'type': 'STRING'},
                                                {'name': 'Step', 'type': 'STRING'},
                                                {'name': 'Step_Date', 'type': 'DATE'},
                                                {'name': 'Recruiting_Hire_Date', 'type': 'DATE'},
                                                {'name': 'Start_Date', 'type': 'DATE'},
                                                {'name': 'Candidate', 'type': 'STRING'},
                                                {'name': 'Candidate_Email_Address', 'type': 'STRING'},
                                                {'name': 'Candidate_Primary_Phone', 'type': 'STRING'},
                                                {'name': 'First_Published_Date', 'type': 'DATE'},
                                                {'name': 'Average_Days_Between_Publish_Hire_Dates', 'type': 'STRING'},
                                                {'name': 'Hospital_System', 'type': 'STRING'}])


if __name__ == '__main__':
    start_time = time.time()
    CHS_ATS()
    print("--- %s seconds ---" % (time.time() - start_time))



