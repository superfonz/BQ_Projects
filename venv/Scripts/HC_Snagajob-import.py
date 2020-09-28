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


def Snagajobimport():  # data, context
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
    labelid = 'Label_4319400225407627751'
    query = 'noreply@lookermail.com'
    messages = service.users().messages().list(userId=userId, q=query, labelIds=labelid).execute()

    def multiple_replace(dict, text):
        regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))
        return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text)

    regexes = {
        'Snag Campaign Leads ': '',
        'Salesforce ': '',
        'Timestamp ': '',
        ' ': '_'
    }

    for m_id in messages['messages']:
        messagemeta = service.users().messages().get(userId=userId, id=m_id['id']).execute()

        dates = parser.parse(re.sub("^.*,|-.*$", "", messagemeta['payload']['headers'][1]['value']).strip()).date()

        today = datetime.today().date() #- timedelta(1)

        if dates == today:

            attachment = messagemeta['payload']['parts'][1]['body']['attachmentId']
            attachments = service.users().messages().attachments().get(userId=userId, messageId=messagemeta['id'],
                                                                       id=attachment).execute()
            f = base64.urlsafe_b64decode(attachments['data'])
            toread = io.BytesIO()
            toread.write(f)
            toread.seek(0)

            dataframe = pd.read_excel(toread, header=0)

            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)

            reformattedcolumns = []

            for column in dataframe.columns:
                reformattedcolumns.append(multiple_replace(regexes, column))

            dataframe.columns = reformattedcolumns
            dataframe['Date'] = pd.to_datetime(dataframe['Date'],
                                               errors='coerce').dt.date

            dataframe.drop(['Account_Name'], inplace=True, axis=1)
            dataframe.dropna(how='any', inplace=True)

            pandas_gbq.to_gbq(dataframe, 'snagajob.Snagajob_spend', project_id='hireclix',
                              if_exists='append', table_schema=[
                                    {'name': 'Date', 'type': 'DATE'}
                                     ])
            dataframe.to_csv('gs://hc_snagajob/snagajob_daily_'+str(today), index=False)
            print(dataframe)


Snagajobimport()
