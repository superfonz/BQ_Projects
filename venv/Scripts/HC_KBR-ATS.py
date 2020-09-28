import pgpy
import os
import gcsfs
from google.cloud import storage
import paramiko
import json
import io
import pandas as pd
import pandas_gbq
from datetime import datetime

# TODO
# Add Schema
# Intergrate GCS FS, IO, BQ, CSV/XLSX?
# Import (FTP) - *(Decrypt)* - (Seek) - Import (Pandas) - Transform - Load (BQ)

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def KBRImport():
    storage_client = storage.Client()

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    data = storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/KBRFTP.json').download_as_string()
    json_data = json.loads(data)

    transport = paramiko.Transport(json_data["host"], 22)
    transport.connect(username=json_data["username"], password=json_data["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Removes Empty Null Char from String/prep for decryt
    x = sftp.open("HireClix_Data.csv.pgp", 'rb').read()
    toread = io.BytesIO()
    toread.write(x)
    toread.seek(0)

    if storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/KBR (FD2E83EC) – Secret.asc').exists():
        with gcsfs.GCSFileSystem(project="hireclix").open('hc_tokens_scripts/Tokens/KBR (FD2E83EC) – Secret.asc',
                                                          'rb') as token:
            creds = pgpy.PGPKey().from_blob(token.read())
            with creds[0].unlock("hireclix10") as ukey:
                message = pgpy.PGPMessage().from_blob(toread.read())
                decryptedmessage = ukey.decrypt(message).message
                decryptedmessagestr = decryptedmessage.decode()
                DMIo = io.StringIO(decryptedmessagestr)
                dataframe = pd.read_csv(DMIo)
    else:
        print("PGP Token not Found, please fix")
        raise FileNotFoundError

    dataframe.rename(columns={'Application Date': 'Application_Date',
                              'Job Requisition ID': 'Job_Requisition_ID',
                              'Job Posting Title': 'Job_Posting_Title',
                              'Job Requisition Primary Location': 'Job_Requisition_Primary_Location',
                              'Job Requisition Status': 'Job_Requisition_Status',
                              'Is Evergreen': 'Is_Evergreen',
                              'First Name': 'First_Name',
                              'Last Name': 'Last_Name',
                              'Candidate ID': 'Candidate_ID',
                              'Candidate Location': 'Candidate_Location',
                              'Candidate Stage': 'Candidate_Stage',
                              'Candidate Step': 'Candidate_Step',
                              'Source': 'Source',
                              'Referred by': 'Referred_by',
                              'Job Code': 'Job_Code',
                              'Security Sub Region': 'Security_Sub_Region'}, inplace=True)
    dataframe['Application_Date'] = pd.to_datetime(dataframe['Application_Date'], errors='coerce').dt.date

    pandas_gbq.to_gbq(dataframe, 'kbr_ats.kbr_atspip_master', project_id='hireclix',
                      if_exists='replace',
                      table_schema=[{'name': 'Application_Date', 'type': 'DATE'},
                                    {'name': 'Job_Requisition_ID', 'type': 'STRING'},
                                    {'name': 'Job_Posting_Title', 'type': 'STRING'},
                                    {'name': 'Job_Requisition_Primary_Location', 'type': 'STRING'},
                                    {'name': 'Job_Requisition_Status', 'type': 'STRING'},
                                    {'name': 'Is_Evergreen', 'type': 'STRING'},
                                    {'name': 'First_Name', 'type': 'STRING'},
                                    {'name': 'Last_Name', 'type': 'STRING'},
                                    {'name': 'Candidate_ID', 'type': 'STRING'},
                                    {'name': 'Candidate_Location', 'type': 'STRING'},
                                    {'name': 'Candidate_Stage', 'type': 'STRING'},
                                    {'name': 'Candidate_Step', 'type': 'STRING'},
                                    {'name': 'Source', 'type': 'STRING'},
                                    {'name': 'Referred_by', 'type': 'STRING'},
                                    {'name': 'Job_Code', 'type': 'STRING'},
                                    {'name': 'Security_Sub_Region', 'type': 'STRING'}])

    today = datetime.today().date()
    dataframe.to_csv('gs://hc_kbr_ats/File_Completed/' + "kbr_data_ " + str(today.month) + "." +
                     str(today.day) + "." + str(today.year) + ".csv", index=False, header=True)


KBRImport()

# key = pgpy.PGPKey().from_file("/Users/alfonzosanfilippo/Desktop/KBR Keys/KBR (FD2E83EC) – Secret.asc")
#
# with key[0].unlock("hireclix10") as ukey:
#     message = pgpy.PGPMessage().from_file("/Users/alfonzosanfilippo/Desktop/Banfield backup.csv.gpg")
#     f = ukey.decrypt(message).message
#     print(f)
