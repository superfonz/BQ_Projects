import base64
import io
import os
import os.path
import pickle
import re
import time
from datetime import datetime, timedelta

import gcsfs
import pandas as pd
import pandas_gbq
from dateutil import parser
from google.auth.transport.requests import Request
from google.cloud import storage
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import json

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def CFCorp():  # data, context
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
    labelid = 'Label_34715380858048669'
    query = 'defaultuserobi@invalidemail.com'
    messages = service.users().messages().list(userId=userId, q=query, labelIds=labelid).execute()

    def multiple_replace(dict, text):
        # Create a regular expression  from the dictionary keys
        regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))
        # For each match, look-up corresponding value in dictionary
        return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text).strip()

    regexes = {
        ' ': '',
        '-': '',
        '(BL)': ''
    }

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

            dataframe = pd.read_csv(toread, header=0)

            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)

            reformattedcolumns = []

            for column in dataframe.columns:
                reformattedcolumns.append(multiple_replace(regexes, column))

            dataframe.columns = reformattedcolumns

            dataframe['SubmissionCompletedDate'] = pd.to_datetime(dataframe['SubmissionCompletedDate'],
                                                                  errors='coerce').dt.date

            pandas_gbq.to_gbq(dataframe, 'CountryFinancial.CountryFinancial_ATS', project_id='hireclix',
                              if_exists='replace', table_schema=[
                    {'name': 'SubmissionCompletedDate', 'type': 'DATE'}
                ])

            dataframe.to_csv('gs://hc_countryfinincialcorp/CF:Corp_ats_' + str(today)+".csv")
            print(dataframe)


CFCorp()
