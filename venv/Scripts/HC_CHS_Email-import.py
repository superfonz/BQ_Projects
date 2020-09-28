import base64
import io
import os
import os.path
import pickle
import re
import time
from datetime import datetime

import gcsfs
import pandas as pd
from dateutil import parser
from google.auth.transport.requests import Request
from google.cloud import storage
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openpyxl as opxl

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def emailimport():  # data, context
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
    labelid = 'Label_1344811892738417502'
    query = 'jessica_connelly@ultipro.com'
    messages = service.users().messages().list(userId=userId, q=query, labelIds=labelid).execute()

    for m_id in messages['messages']:
        messagemeta = service.users().messages().get(userId=userId, id=m_id['id']).execute()
        dates = parser.parse(re.sub("^.*,|-.*$", "", messagemeta['payload']['headers'][1]['value']).strip()).date()
        today = datetime.today().date()
        if dates == today:
            attachment = messagemeta['payload']['parts'][1]['body']['attachmentId']
            attachments = service.users().messages().attachments().get(userId=userId, messageId=messagemeta['id'],
                                                                       id=attachment).execute()
            f = base64.urlsafe_b64decode(attachments['data'])
            toread = io.BytesIO()
            toread.write(f)
            toread.seek(0)

            dataframe = pd.read_excel(toread, header=1)
            dataframe = dataframe.replace(to_replace=r',', value='', regex=True)

            dataframe.rename(columns={'Requisition Number': 'Requisition_Number',
                                      'Opportunity Title': 'Opportunity_Title',
                                      'Opportunity Status': 'Opportunity_Status',
                                      'Featured': 'Featured',
                                      'Company Code': 'Company_Code',
                                      'Company': 'Company',
                                      'Entity': 'Entity',
                                      'Entity Desc': 'Entity_Desc',
                                      'Source Job Code': 'Source_Job_Code',
                                      'Job Title': 'Job_Title',
                                      'Full-Time Or Part-Time': 'FullTime_Or_PartTime',
                                      'Salary Or Hourly': 'Salary_Or_Hourly',
                                      'Recruiter (Last, Suffix First MI)': 'Recruiter',
                                      'Location Name': 'Location_Name',
                                      'Date Applied': 'Date_Applied',
                                      'Source': 'Source',
                                      'Step': 'Step',
                                      'Step Date': 'Step_Date',
                                      'Recruiting Hire Date': 'Recruiting_Hire_Date',
                                      'Start Date': 'Start_Date',
                                      'Candidate (Title First Middle Last Suffix)': 'Candidate',
                                      'Candidate Email Address': 'Candidate_Email_Address',
                                      'Candidate Primary Phone': 'Candidate_Primary_Phone',
                                      'First Published Date': 'First_Published_Date',
                                      'Average(Days Between Publish & Hire Date)': 'Average_Days_Between_Publish_Hire_Dates'},
                             inplace=True)

            dataframe['Date_Applied'] = pd.to_datetime(dataframe['Date_Applied'], errors='coerce').dt.date
            dataframe['Step_Date'] = pd.to_datetime(dataframe['Step_Date'], errors='coerce').dt.date
            dataframe['Recruiting_Hire_Date'] = pd.to_datetime(dataframe['Recruiting_Hire_Date'],
                                                               errors='coerce').dt.date
            dataframe['Start_Date'] = pd.to_datetime(dataframe['Start_Date'], errors='coerce').dt.date
            dataframe['First_Published_Date'] = pd.to_datetime(dataframe['First_Published_Date'],
                                                               errors='coerce').dt.date
            dataframe = dataframe.dropna(how="all")

            dataframe['Company_Code'] = dataframe['Company_Code'].astype(str).replace(r'\.0', '', regex=True)

            dataframe.to_csv(
                'gs://hc_chs_ats/File_Upload/' + "CHS_ATS_DATA " + str(today.month) + "." + str(today.day) +
                "." + str(today.year) + ".csv", index=False, header=True)

if __name__ == '__main__':
    start_time = time.time()
    emailimport()
    print("--- %s seconds ---" % (time.time() - start_time))
