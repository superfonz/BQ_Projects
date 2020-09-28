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
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openpyxl as opxl
import re
import numpy as np

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def KBRImport():
    # ---init---------
    storage_client = storage.Client()

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    credraw = storage_client.get_bucket('hc_tokens_scripts').blob(
        'Tokens/hireclix-googlesheets.json').download_as_string()

    credjson = json.loads(credraw)

    cred = ServiceAccountCredentials.from_json_keyfile_dict(credjson, scope)

    gclient = gspread.authorize(cred)

    sheet = gclient.open_by_key('1nQDxuJVTjfFRSGDIr_eqeT6rJ2yUkZB9kGQWh0GP7d8').worksheet('Automated ATS by BU')
    # -----dev tool-----
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    # -----load STFP server-----
    data = storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/KBRFTP.json').download_as_string()
    json_data = json.loads(data)
    transport = paramiko.Transport(json_data["host"], 22)
    transport.connect(username=json_data["username"], password=json_data["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    x = sftp.open("HireClix_Data.csv.pgp", 'rb').read()
    toread = io.BytesIO()  # Removes Empty Null Char from String/prep for decryt
    toread.write(x)
    toread.seek(0)

    # ------pgp decrypt------
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
    # ----transform data----
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

    # ----pivot table -----
    pivot = dataframe.pivot_table(index=['Application_Date', 'Source', 'Security_Sub_Region', 'Job_Posting_Title',
                                         'Job_Requisition_Primary_Location', 'Job_Requisition_ID'],
                                  columns=['Candidate_Stage'], values=['Candidate_ID'], aggfunc='count')
    pivot.reset_index(inplace=True)
    axes = pivot.axes[1]
    columnheaders = []
    for axis in axes:
        if axis[1] == "":
            columnheaders.append(axis[0])
        elif axis[0] == "Candidate_ID":
            columnheaders.append(axis[1])
        else:
            columnheaders.append("unknown")

    pivotval = pivot.values

    atslst = [columnheaders + ["Month-Year", "Month", "Year", "Applies", "Quality Applies"]]
    ind = 1
    # -----transform pivot for Google sheets-----
    for x in pivotval:
        ind += 1
        month = str(x[0].strftime("%B"))
        year = str(x[0].strftime("%Y"))
        dates = month[0:3] + "-" + year

        ap = '=SUM(INDIRECT("G' + str(ind) + ':P' + str(ind) + '"))'  # insert Applications formula
        qap = '=SUM(INDIRECT("I' + str(ind) + ':M' + str(ind) + '"))'  # insert Quality Applications formula

        templst = []
        for ex in range(0, len(x)):
            templst.append(str(x[ex]))  # format a temp list in the correct manner

        templst.extend([dates, month, year, ap, qap])
        atslst.append(templst)

    for i in range(1, len(atslst)):
        for val in range(5, len(atslst[i])):
            if atslst[i][val] == 'nan':
                atslst[i][val] = ''
            elif re.match('[0-9]+\.[0-9+]', atslst[i][val]):
                atslst[i][val] = int(re.sub("\.[0-9]", "", atslst[i][val]))

    sheet.clear()
    sheet.append_rows(atslst, "USER_ENTERED")

    today = datetime.today().date()
    dataframe.to_csv('gs://hc_kbr_ats/File_Completed/' + "kbr_data_ " + str(today.month) + "." +
                     str(today.day) + "." + str(today.year) + ".csv", index=False, header=True)

    header = ["Application_Date", "Source", "Security_Sub_Region", "Job_Posting_Title",
              "Job_Requisition_Primary_Location", "Job_Requisition_ID", "Background Check", "Declined by Candidate",
              "Hire", "Interview", "Offer", "Offer/Employment Agreement", "Pre-Employment Checklist", "Rejected",
              "Review", "Screen", "Month-Year", "Month", "Year", "Applies", "Quality Applies"]
    mod = 0
    sheetaxis = sheet.get("A1:W1")

    for x in range(0, len(sheetaxis[0])):  # modify header locations post insert
        for i in range(0, len(header)):
            if (i - mod) == x:
                if sheetaxis[0][x] != header[i]:
                    sheet.insert_col([""], i + 1)
                    mod += 1

    sheet.insert_row(header)
    sheet.delete_rows(2, 2)

if __name__ = '__main__':
    KBRImport()

# key = pgpy.PGPKey().from_file("/Users/alfonzosanfilippo/Desktop/KBR Keys/KBR (FD2E83EC) – Secret.asc")
#
# with key[0].unlock("hireclix10") as ukey:
#     message = pgpy.PGPMessage().from_file("/Users/alfonzosanfilippo/Desktop/Banfield backup.csv.gpg")
#     f = ukey.decrypt(message).message
#     print(f)
