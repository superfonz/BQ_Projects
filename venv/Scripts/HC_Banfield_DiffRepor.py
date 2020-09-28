import pandas as pd
import pandas_gbq
import base64
import io
import os
import os.path
import pickle
import re
import time
from datetime import datetime, timedelta
from google.api_core import exceptions

import gcsfs
import pandas as pd
from dateutil import parser
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.cloud import bigquery, bigquery_storage_v1beta1, storage


os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def BanfieldEmailimport():
    bigquery_client = bigquery.Client()
    biqquery_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
    creds = None
    storage_client = storage.Client()
    if storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/Reporting-token.pickle').exists():
        with gcsfs.GCSFileSystem(project="hireclix").open('hc_tokens_scripts/Tokens/Reporting-token.pickle',
                                                          'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        with gcsfs.GCSFileSystem(project="hireclix").open('hc_tokens_scripts/Tokens/Reporting-token.pickle',
                                                          'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)
    userId = 'me'
    labelid = 'Label_1029730425169340541'  # for reporting pickle
    query = 'defaultuserobi@invalidemail.com'
    messages = service.users().messages().list(userId=userId, q=query, labelIds=labelid).execute()

    for m_id in messages['messages']:
        messagemeta = service.users().messages().get(userId=userId, id=m_id['id']).execute()
        dates = parser.parse(re.sub("^.*,|-.*$", "", messagemeta['payload']['headers'][1]['value']).strip()).date()
        today = datetime.today().date() - timedelta(days=1)
        if dates == today:
            attachment = messagemeta['payload']['parts'][1]['body']['attachmentId']
            attachments = service.users().messages().attachments().get(userId=userId, messageId=messagemeta['id'],
                                                                       id=attachment).execute()
            f = base64.urlsafe_b64decode(attachments['data'])
            toread = io.BytesIO()
            toread.write(f)
            toread.seek(0)

            dataframe = pd.read_excel(toread, header=2, dtype=str)
            dataframe = dataframe.replace(to_replace=r',', value='', regex=True)

            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)

            dataframe.rename(columns={'Candidate Identifier': 'Candidate_Identifier',
                                      'Submission Created Date': 'Submission_Created_Date',
                                      'Submission Source (BL)': 'Submission_Source',
                                      'Submission Source Identifier': 'Submission_Source_Identifier',
                                      'Requisition Number': 'Requisition_Number',
                                      'Title (BL)': 'Title',
                                      'Current Status': 'Current_Status',
                                      'Current Step Name': 'Current_Step_Name',
                                      'Current Status Name': 'Current_Status_Name',
                                      'Current Status Start Date': 'Current_Status_Start_Date',
                                      'Region': 'Region',
                                      'Market': 'Market',
                                      'State - Name': 'State',
                                      'Location Level4 - Name': 'City',
                                      'Recruiter Name': 'Recruiter_Name',
                                      'Submission Is Completed': 'Submission_Is_Completed',
                                      'Posting Date': 'Posting_Date'}, inplace=True)

            dataframe['Candidate_Identifier'].ffill(inplace=True)
            dataframe['Current_Status_Start_Date'].ffill(inplace=True)

            dataframe['Candidate_Identifier'] = dataframe['Candidate_Identifier'].astype(str).replace(r'\.0', '',
                                                                                                      regex=True)

            dataframe['Submission_Created_Date'] = pd.to_datetime(dataframe['Submission_Created_Date'],
                                                                  errors='coerce').dt.date
            dataframe['Current_Status_Start_Date'] = ""

            dataframe['Posting_Date'] = ""

            dataframe.dropna(how="all", inplace=True)

            query_string = """ 
                        SELECT 
                        * 
                        FROM 
                        `hireclix.banfield.banfield_ATS`
                        """
            try:
                print("Requesting")
                # Request data from BigQuery
                bgdata = (
                    bigquery_client.query(query_string).result().to_dataframe(bqstorage_client=biqquery_storage_client)
                    # convert to dataframe
                )
                bgdata.to_csv("bgdata.csv",index=False,header=True)
                print("Reformating Dataframe")
                merged_df = bgdata.append(dataframe, True)  # Append dataframe together

                merged_df.drop_duplicates(keep='last', subset=[
                    'Candidate_Identifier', 'Submission_Created_Date', 'Submission_Source',
                    'Submission_Source_Identifier',
                    'Requisition_Number', 'Title', 'Region', 'Market', 'State', 'City', 'Recruiter_Name',
                    'Submission_Is_Completed'
                ], inplace=True)

            except exceptions.NotFound:
                print("Big query Table not Found, Creating New one")
            raise KeyboardInterrupt

            print(merged_df)
            merged_df.to_csv("diffrep.csv",index=False, header=True)
            raise KeyboardInterrupt

            pandas_gbq.to_gbq(merged_df, 'banfield.banfield_ATS', project_id='hireclix',
                              if_exists='replace',
                              table_schema=[{'name': 'Candidate_Identifier', 'type': 'STRING'},
                                            {'name': 'Submission_Created_Date', 'type': 'DATE'},
                                            {'name': 'Submission_Source', 'type': 'STRING'},
                                            {'name': 'Submission_Source_Identifier', 'type': 'STRING'},
                                            {'name': 'Requisition_Number', 'type': 'STRING'},
                                            {'name': 'Title', 'type': 'STRING'},
                                            {'name': 'Current_Status', 'type': 'STRING'},
                                            {'name': 'Current_Step_Name', 'type': 'STRING'},
                                            {'name': 'Current_Status_Name', 'type': 'STRING'},
                                            {'name': 'Current_Status_Start_Date', 'type': 'DATE'},
                                            {'name': 'Region', 'type': 'STRING'},
                                            {'name': 'Market', 'type': 'STRING'},
                                            {'name': 'State', 'type': 'STRING'},
                                            {'name': 'City', 'type': 'STRING'},
                                            {'name': 'Recruiter_Name', 'type': 'STRING'},
                                            {'name': 'Submission_Is_Completed', 'type': 'STRING'}])

            dataframe.to_csv(
                'gs://hc_banfield_ats/File_Completed/' + "Banfield_ATS_DATA " + str(today.month) + "." + str(
                    today.day) +
                "." + str(today.year) + ".csv", index=False, header=True)

            merged_df.to_csv(
                'gs://hc_banfield_ats/File_Backup/' + "Banfield_ATS_DATA " + str(today.month) + "." + str(today.day) +
                "." + str(today.year) + "_merged.csv", index=False, header=True)


if __name__ == "__main__":
    BanfieldEmailimport()
