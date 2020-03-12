#####################################################  HEADER  #######################################################
#
#   NAME: HireClix - CrackerBarrel ATS to BigQuery Data Migration
#   AUTHOR: Alfonzo Sanfilippo <Alfonzo.Sanfilippo@HireClix.com>
#   DATE_CREATED: March 9, 2020
#   VERSION: 0.1.0
#   COPYRIGHT: Copyright 2020, HireClix DataBridge
#   DESCRIPTION:
#       This program uses Pandas (Python Data Analysis Library <https://pandas.pydata.org/docs/user_guide/index.html>)
#       and the Google-Cloud Suite to Extract data from a text file (.csv, xlsx, etc) and Clean/Transform the data to be
#       Loaded on BigQuery. (ETL <https://en.wikipedia.org/wiki/Extract,_transform,_load>)
#
######################################################################################################################

# <-----------------------------------------------------CONSTANTS----------------------------------------------------->

# Google Cloud Storage API <https://gcsfs.readthedocs.io/en/latest/>
import gcsfs

# Miscellaneous operating system interfaces <https://docs.python.org/3/library/os.html>
import os

# Regular expression operations <https://docs.python.org/3/library/re.html>
import re

# Google Authentication <https://google-auth.readthedocs.io/en/latest/reference/google.auth.credentials.html>
import google.auth

# Basic date and time types <https://docs.python.org/2/library/datetime.html>
from datetime import datetime, timedelta

# Python Data Analysis Library <https://pandas.pydata.org/docs/user_guide/index.html>
import pandas as pd

# Pandas Extension For Google BigQuery <https://pandas-gbq.readthedocs.io/en/latest/intro.html>
import pandas_gbq

# Google Cloud Suite <https://cloud.google.com/python/docs
from google.api_core import exceptions

# GCS - BigQuery API Extensions
#   (bigquery <https://cloud.google.com/bigquery/docs/reference/libraries>)
#   (bigquery_storage_v1beta1 <https://googleapis.dev/python/bigquerystorage/latest/gapic/v1beta1/api.html>)
# GCS - Data Store API Extension (storage <https://cloud.google.com/storage/docs/reference/libraries>)
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

# Authenticating the environment by explicitly loading Google Provided Credentials found in the Resource folder called
# "hireclix.json" file

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


# <---------------------------------------------------MAIN FUNCTION--------------------------------------------------->

def CB_ATS_Data():  # <- (data, context) needs to be passed on Google Cloud Functions

    # <----DESCRIPTION---->
    # Iterate through files in the bucket "hc_crackerbarrel_ats" in the folder "File_Upload/"
    # and locates the file extension of ".xlsx". If the file is found then it will load it into
    # a dataframe, Clean the data with the specified Transformations, then query existing BigQuery table
    # and append them together while dropping any line items that are duplicates according to the subset.
    # <------------------->

    # <---Authenticate and initialize--->
    # Data Store API Client
    storage_client = storage.Client()

    # BigQuery API Client
    bigquery_client = bigquery.Client()

    #  BigQuery API Extension Client
    biqquery_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
    # <--------------------------------->

    # iterate through the files on the Data Store
    for blob in storage_client.list_blobs("hc_crackerbarrel_ats", prefix="File_Upload/"):

        # match file extension ".xlsx"
        match = re.search("/(.+?).xlsx", str(blob))

        if match:
            # <----DEV TOOLS----> Gives a better print out from pandas in the console.
            # pd.set_option('display.max_rows', 500)
            # pd.set_option('display.max_columns', 500)
            # pd.set_option('display.width', 1000)
            # <----------------->

            # Read and import .xlsx file and store in a dataframe
            dataframe = pd.read_excel('gs://hc_crackerbarrel_ats/File_Upload/' + match.group(1) + ".xlsx",
                                      sheet_name="Sheet1")

            # <-----------------------------------------EXTRA TRANSFORMATIONS----------------------------------------->
            # dataframe = dataframe.replace({'Job : Position Title': r'-.*$|,.*$'}, {'Job : Position Title': ''},
            #                              regex=True)
            # dataframe = dataframe.replace({'Job : Position Title': r'-.*$|,.*$'}, {'Job : Position Title': ''},
            #                              regex=True)
            # <------------------------------------------------------------------------------------------------------->

            # <---TRANSFORMATIONS--->
            # Delete any commas in the dataframe, using RegEx
            dataframe = dataframe.replace(to_replace=r',', value='', regex=True)

            # Rename Imported Columns to BigQuery Accepted Columns - No Spaces and no Special Characters
            dataframe.rename(columns={'Person : Full Name: First Last (Non-Sortable)': 'Person_FullName_FirstLast',
                                      'Application Date': 'ApplicationDate',
                                      'Job : Position Title': 'Job_PositionTitle', 'Source Channel': 'SourceChannel',
                                      'Source': 'Source', 'Source Name': 'SourceName',
                                      'Location : Store #': 'Location_Store', 'Location : City': 'Location_City',
                                      'Location : State/Province': 'Location_StateProvince',
                                      'First New Submissions': 'FirstNewSubmissions',
                                      'First Interview': 'FirstInterview', 'First Hired': 'FirstHired',
                                      'Current Application Step': 'CurrentApplicationStep',
                                      'Job : Requisition ID': 'Job_RequisitionID'}, inplace=True)

            # Convert Date Strings into Date Objects - BigQuery only accepts specific Date DataTypes
            dataframe['ApplicationDate'] = pd.to_datetime(dataframe['ApplicationDate'], errors='coerce').dt.date
            dataframe['FirstNewSubmissions'] = pd.to_datetime(dataframe['FirstNewSubmissions'], errors='coerce').dt.date
            dataframe['FirstInterview'] = pd.to_datetime(dataframe['FirstInterview'], errors='coerce').dt.date
            dataframe['FirstHired'] = pd.to_datetime(dataframe['FirstHired'], errors='coerce').dt.date

            # Convert Numeric Objects into Strings - Improper Data Type
            dataframe['Job_RequisitionID'] = dataframe['Job_RequisitionID'].astype('str')
            # <--------------------->

            # String to Query BigQuery Table / Uses SQL Format -
            # Below Basically Means Everything from the Specific Table

            query_string = """ 
            SELECT 
            * 
            FROM 
            `hireclix.crackerbarrel.crackerbarrel_ats`
            """

            # Try to extract existing BigQuery table and append incoming data if it exists,
            # (Append order matters only for Quality, <New Data under existing data>)
            # otherwise create a new one

            try:
                # Request data from BigQuery
                bgdata = (
                    bigquery_client.query(query_string)
                        .result()
                        .to_dataframe(bqstorage_client=biqquery_storage_client)  # convert to dataframe
                )
                dataframe = bgdata.append(dataframe, True)  # Append dataframe together
                dataframe = dataframe.drop_duplicates(  # Drop Duplicates according to joining keys (subset)
                    subset=['Person_FullName_FirstLast', 'ApplicationDate', 'Job_PositionTitle', 'Location_Store',
                            'Location_City', 'Location_StateProvince', 'Job_RequisitionID'], keep="last")

            except exceptions.NotFound:
                print("Big query Table not Found, Creating New one")

            # <----DEV TOOL----> Prints Full table Locally in CSV Format
            # dataframe.to_csv("test.csv", index=False)
            # <---------------->

            # Exports dataframe to Big Query
            # Schema must be in Dict format - Ex: {'name': '{COLUMN NAME}', 'type': '{DATA TYPE}'}
            # if_exist options - replace, append, fail - fail is default
            pandas_gbq.to_gbq(dataframe, 'crackerbarrel.crackerbarrel_ats', project_id='hireclix',
                              if_exists='replace',
                              table_schema=[{'name': 'Person_FullName_FirstLast', 'type': 'STRING'},
                                            {'name': 'ApplicationDate', 'type': 'DATE'},
                                            {'name': 'Job_PositionTitle', 'type': 'STRING'},
                                            {'name': 'SourceChannel', 'type': 'STRING'},
                                            {'name': 'Source', 'type': 'STRING'},
                                            {'name': 'SourceName', 'type': 'STRING'},
                                            {'name': 'Location_Store', 'type': 'INTEGER'},
                                            {'name': 'Location_City', 'type': 'STRING'},
                                            {'name': 'Location_StateProvince', 'type': 'STRING'},
                                            {'name': 'FirstNewSubmissions', 'type': 'DATE'},
                                            {'name': 'FirstInterview', 'type': 'DATE'},
                                            {'name': 'FirstHired', 'type': 'DATE'},
                                            {'name': 'CurrentApplicationStep', 'type': 'STRING'},
                                            {'name': 'Job_RequisitionID', 'type': 'STRING'}])

            # export dataframe to File_Backup/ folder in the datastore, with today's date
            dataframe.to_csv(
                'gs://hc_crackerbarrel_ats/File_Backup/' + match.group(1) + "_" +
                str(datetime.today().date()) + "_Merged.csv", index=False)

            # Move imported file from File_Upload/ to File_Completed/
            # There isn't a specific function in the python version of Data Store API that moves the file
            # So instead of "moving" the file, im copying it to another folder then deleted the file in the old folder
            found = match.group(1) + ".xlsx"
            source_bucket = storage_client.get_bucket("hc_crackerbarrel_ats")
            source_blob = source_bucket.blob("File_Upload/" + found)
            destination_bucket = storage_client.get_bucket("hc_crackerbarrel_ats")
            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
            source_blob.delete()


# Calls the function / without this nothing will run because all the programing is stored in a function.
# this method making transferring the local function to the Google Functions with little to no manipulation
CB_ATS_Data()
