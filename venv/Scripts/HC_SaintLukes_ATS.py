import os
import gcsfs
from google.cloud import storage
import paramiko
import json
import io
import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openpyxl as opxl
import re
import numpy as np
import csv

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def StlukesATSimport():
    storage_client = storage.Client()

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    credraw = storage_client.get_bucket('hc_tokens_scripts').blob(
        'Tokens/hireclix-googlesheets.json').download_as_string()

    credjson = json.loads(credraw)

    cred = ServiceAccountCredentials.from_json_keyfile_dict(credjson, scope)

    gclient = gspread.authorize(cred)

    sheet = gclient.open_by_key('1Eera1pdadOGGg091WKtU05j1-N1zhCKfN3ppWTuvjaA').worksheet('ATS Automation')

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    data = storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/stlukesFTP.json').download_as_string()
    json_data = json.loads(data)

    transport = paramiko.Transport(json_data["host"], 22)
    transport.connect(username=json_data["username"], password=json_data["password"])
    sftp = paramiko.SFTPClient.from_transport(transport)

    with sftp.open("SLHS_applications_yyyymmdd.csv", 'r') as fl:
        dataframe = pd.read_csv(fl, dtype=str)

    dataframe['Date_Candidate_Registered'] = pd.to_datetime(dataframe['Date_Candidate_Registered'],
                                                            errors='coerce').dt.date
    dataframe['Date_Applied'] = pd.to_datetime(dataframe['Date_Applied'], errors='coerce').dt.date
    dataframe['Stage_Effective_Date'] = pd.to_datetime(dataframe['Stage_Effective_Date'], errors='coerce').dt.date
    dataframe['Hire_Start_Date'] = pd.to_datetime(dataframe['Hire_Start_Date'], errors='coerce').dt.date

    dataframe = dataframe.astype({'Count_of_Candidate_Applications': int,
                                  'Date_Candidate_Registered': str,
                                  'Date_Applied': str,
                                  'Stage_Effective_Date': str,
                                  'Hire_Start_Date': str})

    dataframe[['Source_Type', 'Source']] = dataframe['source'].str.split('->', expand=True)

    dataframe.drop('source', axis=1, inplace=True)
    dataframe['Source_Type'] = dataframe['Source_Type'].str.strip()
    dataframe['Source'] = dataframe['Source'].str.strip()

    dataframe['Job_Requisition'] = dataframe['Job_Requisition'].replace(r'\s.*$', '', regex=True)

    cols = dataframe.columns.tolist()  # Reshuffle Columns
    cols.insert(6, cols.pop(cols.index('Source')))
    cols.insert(6, cols.pop(cols.index('Source_Type')))
    dataframe = dataframe.reindex(columns=cols)
    dataframe.fillna('', inplace=True)
    dataframe['Hire_Start_Date'] = dataframe['Hire_Start_Date'].replace('NaT', '')

    sheetdata = sheet.get_all_values()
    existingdf = pd.DataFrame(sheetdata[1:], columns=sheetdata[0])
    existingdf = existingdf.append(dataframe, ignore_index=True)
    existingdf.drop_duplicates(inplace=True, keep='last', subset=['Date_Candidate_Registered', 'Date_Applied',
                                                                  'Job_Requisition', 'Job_Family', 'Job_Profile',
                                                                  'Source_Type', 'Source', 'Job_Posting_Title',
                                                                  'candidate'])
    existingdf.reset_index(drop=True)

    atslst = [cols] + existingdf.values.tolist()

    sheet.clear()
    sheet.append_rows(atslst, "USER_ENTERED")

    today = datetime.today().date()
    dataframe.to_csv('gs://hc_saintlukes_ats/File_Completed/' + "Saint-LukesData_" + str(today.month) + "." +
                     str(today.day) + "." + str(today.year) + ".csv", index=False, header=True)

    existingdf.to_csv('gs://hc_saintlukes_ats/File_Backup/' + "Saint-LukesData_" + str(today.month) + "." +
                     str(today.day) + "." + str(today.year) + "_Merged.csv", index=False, header=True)


StlukesATSimport()
