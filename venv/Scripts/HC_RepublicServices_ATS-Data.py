import os
from google.cloud import storage
import json
import paramiko
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def smartfileRS():
    storage_client = storage.Client()
    data = storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/RepublicServicesFTP.json').download_as_string()
    json_data = json.loads(data)

    transport = paramiko.Transport(json_data["host"], 22)
    transport.connect(username=json_data["username"], password=json_data["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    with sftp.open('Candidate Source Report.csv', "r") as fl:
        dataframe = pd.read_csv(fl, error_bad_lines=False, delimiter=',', names=list('abcdefghijkl'), dtype=str)
        dataframe = dataframe.drop('l', axis=1)
        dataframe = dataframe.drop(dataframe.index[0])
        dataframe.rename(columns={'a': 'Source',
                                  'b': 'ReferralSource',
                                  'c': 'JobApplicationSource',
                                  'd': 'JobProfile',
                                  'e': 'JobPostingLocation',
                                  'f': 'JobPrimaryLocation',
                                  'g': 'RecruitingStage',
                                  'h': 'AppliedDate',
                                  'i': 'CandidateHireDate',
                                  'j': 'CurrentPositionStartDate',
                                  'k': 'IsEvergreen'}, inplace=True)
        dataframe['Source'] = dataframe['Source'].replace(to_replace=r'^.*>', value='', regex=True).str.strip()

        dataframe['AppliedDate'] = pd.to_datetime(dataframe['AppliedDate'], errors='coerce').dt.date
        dataframe['CandidateHireDate'] = pd.to_datetime(dataframe['CandidateHireDate'], errors='coerce').dt.date
        dataframe['CurrentPositionStartDate'] = pd.to_datetime(dataframe['CurrentPositionStartDate'], errors='coerce').dt.date
        dataframe['IsEvergreen'] = pd.to_datetime(dataframe['IsEvergreen'], errors='coerce').dt.date

        pandas_gbq.to_gbq(dataframe, 'republicservices_ats.ats_master', project_id='hireclix',
                          if_exists='replace',
                          table_schema=[{'name': 'Source', 'type': 'STRING'},
                                        {'name': 'ReferralSource', 'type': 'STRING'},
                                        {'name': 'JobApplicationSource', 'type': 'STRING'},
                                        {'name': 'JobProfile', 'type': 'STRING'},
                                        {'name': 'JobPostingLocation', 'type': 'STRING'},
                                        {'name': 'JobPrimaryLocation', 'type': 'STRING'},
                                        {'name': 'RecruitingStage', 'type': 'STRING'},
                                        {'name': 'AppliedDate', 'type': 'DATE'},
                                        {'name': 'CandidateHireDate', 'type': 'DATE'},
                                        {'name': 'CurrentPositionStartDate', 'type': 'DATE'},
                                        {'name': 'IsEvergreen', 'type': 'DATE'}])
        dataframe.to_csv(
            'gs://hc_republicservices_ats/Candidate Source Report_' +
            str(datetime.today().date()) + "_Merged.csv", index=False)
        sftp.close()

smartfileRS()
