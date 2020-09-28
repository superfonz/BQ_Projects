import os
from google.cloud import storage
import json
import paramiko
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def smartfileCC():
    # -----Grab Creds------
    storage_client = storage.Client()
    data = storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/ConsumerCellularFTP.json').download_as_string()
    json_data = json.loads(data)

    # ----------Connect to SFTP Server----------
    transport = paramiko.Transport(json_data["host"], 22)
    transport.connect(username=json_data["username"], password=json_data["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    # --------Dev tool--------
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    # -----------Extract Data---------
    for FileName in sftp.listdir():
        lstatout = str(sftp.lstat(FileName)).split()[0]
        if 'd' not in lstatout:
            with sftp.open(FileName, "r") as FileStream:
                dataframe = pd.read_excel(FileStream)

                dataframe = dataframe.replace(to_replace=r',', value='', regex=True)
                dataframe.to_csv('gs://hc_consumercellular_ats/File_Completed/'+FileName)

                dataframe.rename(columns={"Candidates - Id": "CandidatesID",
                                          "Requisitions - Id": "RequisitionsId",
                                          "Requisitions - Title": "RequisitionsTitle",
                                          "Requisitions - Location": "RequisitionsLocation",
                                          "Candidates - City": "CandidatesCity",
                                          "Candidates - Stateterritory": "CandidatesStateterritory",
                                          "Requisitions - Opened Date": "RequisitionsOpenedDate",
                                          "Candidates - Email": "CandidatesEmail",
                                          "Candidates - Phone": "CandidatesPhone",
                                          "Requisition Candidates - Date Applied": "RequisitionCandidatesDateApplied",
                                          "Requisition Candidates - Status": "RequisitionCandidatesStatus",
                                          "Requisition Candidates - In Status Date": "RequisitionCandidatesInStatusDate",
                                          "Requisition Candidates - Pre-rejection Status": "RequisitionCandidatesPrerejectionStatus",
                                          "Candidates - Source": "CandidatesSource",
                                          "Candidates - Start Date": "CandidatesStartDate",
                                          "Candidates - Recruiter": "CandidatesRecruiter"}, inplace=True)

                dataframe['CandidatesID'] = dataframe['CandidatesID'].astype(str).replace(r'\.0', '', regex=True)
                dataframe['RequisitionsTitle'] = dataframe['RequisitionsTitle'].replace(r' \(.*\)', '', regex=True)
                dataframe['RequisitionsOpenedDate'] = pd.to_datetime(dataframe['RequisitionsOpenedDate'], errors='coerce').dt.date
                dataframe['RequisitionCandidatesDateApplied'] = pd.to_datetime(dataframe['RequisitionCandidatesDateApplied'], errors='coerce').dt.date
                dataframe['RequisitionCandidatesInStatusDate'] = pd.to_datetime(dataframe['RequisitionCandidatesInStatusDate'], errors='coerce').dt.date
                dataframe['CandidatesStartDate'] = pd.to_datetime(dataframe['CandidatesStartDate'], errors='coerce').dt.date
                dataframe['CandidatesPhone'] = dataframe['CandidatesPhone'].astype(str).replace(r'-|\(|\)| ', '', regex=True)
                dataframe['CandidatesPhone'] = dataframe['CandidatesPhone'].apply(lambda x: x[:-10]+'('+x[-10:-7]+')'+x[-7:-4]+'-'+x[-4:] if x != 'nan' else x)
    # ----- Load data to Bigquery------

                print("toBQ")
                pandas_gbq.to_gbq(dataframe, 'consumercellular_ats.consumercellular_ats_master', project_id='hireclix',
                                  if_exists='replace',
                                  table_schema=[{'name': 'CandidatesID', 'type': 'STRING'},
                                                {'name': 'RequisitionsId', 'type': 'STRING'},
                                                {'name': 'RequisitionsTitle', 'type': 'STRING'},
                                                {'name': 'RequisitionsLocation', 'type': 'STRING'},
                                                {'name': 'CandidatesCity', 'type': 'STRING'},
                                                {'name': 'CandidatesStateterritory', 'type': 'STRING'},
                                                {'name': 'RequisitionsOpenedDate', 'type': 'DATE'},
                                                {'name': 'CandidatesEmail', 'type': 'STRING'},
                                                {'name': 'CandidatesPhone', 'type': 'STRING'},
                                                {'name': 'RequisitionCandidatesDateApplied', 'type': 'DATE'},
                                                {'name': 'RequisitionCandidatesStatus', 'type': 'STRING'},
                                                {'name': 'RequisitionCandidatesInStatusDate', 'type': 'DATE'},
                                                {'name': 'RequisitionCandidatesPrerejectionStatus', 'type': 'STRING'},
                                                {'name': 'CandidatesSource', 'type': 'STRING'},
                                                {'name': 'CandidatesStartDate', 'type': 'DATE'},
                                                {'name': 'CandidatesRecruiter', 'type': 'STRING'}])
                print("Fix GCS")
                dataframe.to_csv('gs://hc_consumercellular_ats/File_Backup/'+FileName+'_'+str(datetime.today().date())+".csv")
                sftp.rename("/" + FileName, "/Completed/" + FileName)


smartfileCC()
