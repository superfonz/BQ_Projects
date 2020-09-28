import gcsfs
import os
import re
import google.auth
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
from google.api_core import exceptions
from google.cloud import bigquery, bigquery_storage_v1beta1, storage

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def CHS_indeed_Spend():
    client = storage.Client()  # Storage
    bqclient = bigquery.Client()  # Readable BQ
    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient()  # writable to BQ

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    for blob in client.list_blobs("hc_chs_indeed-spend", prefix="File_Upload/"):
        f = re.search("/(.+?).csv", str(blob))
        if f:
            DateField = re.search("[0-9]+-[0-9]+-[0-9]+", str(f.group(1))).group(0)

            dataframe = pd.read_csv('gs://hc_chs_indeed-spend/File_Upload/' + f.group(1) + ".csv", header=1)
            dataframe = dataframe.drop(dataframe.index[0])
            if dataframe.empty:
                continue
            dataframe['Date'] = DateField
            dataframe['Date'] = pd.to_datetime(dataframe['Date'], errors='coerce').dt.date
            dataframe['Last updated date'] = pd.to_datetime(dataframe['Last updated date'], errors='coerce').dt.date
            dataframe['First indexed date'] = pd.to_datetime(dataframe['First indexed date'], errors='coerce').dt.date
            dataframe['Source'] = "Indeed"
            dataframe['Source'] = dataframe['Source'].astype('str')

            dataframe.rename(columns={'Job title': 'Job_title',
                                      'Reference number': 'Reference_number',
                                      'Source website': 'Source_website',
                                      'Last updated date': 'Last_updated_date',
                                      'First indexed date': 'First_indexed_date',
                                      'Impressions Sponsored': 'Impressions_Sponsored',
                                      'Impressions Combine': 'Impressions_Combine',
                                      'Clicks Sponsored': 'Clicks_Sponsored',
                                      'Clicks Organic': 'Clicks_Organic',
                                      'Clicks Combined': 'Clicks_Combined',
                                      'Impressions Organic': 'Impressions_Organic',
                                      'Impressions Combined': 'Impressions_Combined',
                                      'Sponsored Applies Sponsored': 'Sponsored_Applies_Sponsored',
                                      'CTR Sponsored': 'CTR_Sponsored',
                                      'CTR Organic': 'CTR_Organic',
                                      'CTR Combined': 'CTR_Combined',
                                      'Sponsored Apply rate Sponsored': 'Sponsored_Apply_rate_Sponsored',
                                      'AVG CPC': 'AVG_CPC',
                                      'AVG CPA': 'AVG_CPA',
                                      'Total cost': 'Total_cost'}, inplace=True)

            dataframe = dataframe.replace(to_replace=r',', value='', regex=True)

            dataframe = dataframe.replace({'CTR_Sponsored': r'%'}, {'CTR_Sponsored': ''}, regex=True)
            dataframe = dataframe.replace({'CTR_Organic': r'%'}, {'CTR_Organic': ''}, regex=True)
            dataframe = dataframe.replace({'CTR_Combined': r'%'}, {'CTR_Combined': ''}, regex=True)
            dataframe = dataframe.replace({'Sponsored_Apply_rate_Sponsored': r'%'},
                                          {'Sponsored_Apply_rate_Sponsored': ''}, regex=True)

            dataframe = dataframe.astype({'CTR_Sponsored': float,
                                          'CTR_Organic': float,
                                          'CTR_Combined': float,
                                          'Sponsored_Apply_rate_Sponsored': float,
                                          'AVG_CPC': float,
                                          'AVG_CPA': float,
                                          'Total_cost': float})

            dataframe['CTR_Sponsored'] = dataframe['CTR_Sponsored'] / 100
            dataframe['CTR_Organic'] = dataframe['CTR_Organic'] / 100
            dataframe['CTR_Combined'] = dataframe['CTR_Combined'] / 100
            dataframe['Sponsored_Apply_rate_Sponsored'] = dataframe['Sponsored_Apply_rate_Sponsored'] / 100
            dataframe = dataframe.round(2)

            print(dataframe)
            # query_string = """
            #             SELECT
            #             *
            #             FROM
            #             `hireclix.chs.indeed_master`
            #             """
            # try:
            #     print("Retrieving data")
            #     bgdata = (
            #         bqclient.query(query_string)
            #             .result()
            #             .to_dataframe(bqstorage_client=bqstorageclient)
            #     )
            #     dataframe = bgdata.append(dataframe, True)
            #     print("dedup")
            #     dataframe = dataframe.drop_duplicates(
            #         subset=['Job_title', 'Location', 'Company', 'Reference_number', 'Source_website',
            #                 'URL', 'Date', 'Source'], keep="last")
            # except exceptions.NotFound:
            #     print("Big query Table not Found, Creating New one")

            print("to bq")
            pandas_gbq.to_gbq(dataframe, 'chs.indeed_master', project_id='hireclix',
                              if_exists='append',
                              table_schema=[{'name': 'Status', 'type': 'STRING'},
                                            {'name': 'Job_title', 'type': 'STRING'},
                                            {'name': 'Location', 'type': 'STRING'},
                                            {'name': 'Company', 'type': 'STRING'},
                                            {'name': 'Reference_number', 'type': 'STRING'},
                                            {'name': 'Source_website', 'type': 'STRING'},
                                            {'name': 'Last_updated_date', 'type': 'DATE'},
                                            {'name': 'First_indexed_date', 'type': 'DATE'},
                                            {'name': 'URL', 'type': 'STRING'},
                                            {'name': 'Impressions_Sponsored', 'type': 'INTEGER'},
                                            {'name': 'Impressions_Organic', 'type': 'INTEGER'},
                                            {'name': 'Impressions_Combined', 'type': 'INTEGER'},
                                            {'name': 'Clicks_Sponsored', 'type': 'INTEGER'},
                                            {'name': 'Clicks_Organic', 'type': 'INTEGER'},
                                            {'name': 'Clicks_Combined', 'type': 'INTEGER'},
                                            {'name': 'Sponsored_Applies_Sponsored', 'type': 'INTEGER'},
                                            {'name': 'CTR_Sponsored', 'type': 'FLOAT'},
                                            {'name': 'CTR_Organic', 'type': 'FLOAT'},
                                            {'name': 'CTR_Combined', 'type': 'FLOAT'},
                                            {'name': 'Sponsored_Apply_rate_Sponsored', 'type': 'FLOAT'},
                                            {'name': 'AVG_CPC', 'type': 'FLOAT'},
                                            {'name': 'AVG_CPA', 'type': 'FLOAT'},
                                            {'name': 'Total_cost', 'type': 'FLOAT'},
                                            {'name': 'Date', 'type': 'DATE'},
                                            {'name': 'Source', 'type': 'STRING'}])
            # print("fixing GCS")
            # dataframe.to_csv(
            #     'gs://hc_chs_indeed-spend/File_Backup/' + f.group(1) + "_" +
            #     str(datetime.today().date()) + "_Merged.csv", index=False)
            found = f.group(1) + ".csv"
            source_bucket = client.get_bucket("hc_chs_indeed-spend")
            source_blob = source_bucket.blob("File_Upload/" + found)
            destination_bucket = client.get_bucket("hc_chs_indeed-spend")
            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
            source_blob.delete()


CHS_indeed_Spend()
