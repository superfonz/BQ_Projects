from __future__ import absolute_import

import argparse
import logging
import re
import os
import pandas as pd
import gcsfs

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import bigquery
from apache_beam.io.filesystems import FileSystems
from apache_beam.io import fileio
from apache_beam.io.gcp import gcsfilesystem

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
import google.auth

from datetime import datetime, timedelta
from threading import Timer

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'


def run(argv=None):
    class DataIngestion:
        def parse_method(self, string_input):
            values = re.split(",", re.sub('\r\n', '', re.sub(u'"', '', string_input)))

            row = dict(
                zip(('Requisition_Number', 'Opportunity_Title', 'Opportunity_Status', 'Featured', 'Company_Code',
                     'Company', 'Entity', 'Entity_Desc', 'Source_Job_Code', 'Job_Title', 'FullTime_Or_PartTime',
                     'Salary_Or_Hourly', 'Recruiter', 'Location_Name', 'Date_Applied', 'Source', 'Step', 'Step_Date',
                     'Recruiting_Hire_Date', 'Start_Date', 'Candidate', 'Candidate_Email_Address',
                     'Candidate_Primary_Phone', 'First_Published_Date', 'Average_Days_Between_Publish_Hire_Dates'),
                    values))
            return row

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
             'a file in a Google Storage Bucket.',
        # default='gs://hc_crackerbarrel_ats/Historical'
    )
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='chs.ats_master')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=hireclix',
        '--region=us-east1',
        '--staging_location=gs://hc_chs_ats/File_Temp/Source',
        '--temp_location=gs://hc_chs_ats/File_Temp/Staging',
        '--job_name=chstest1'
    ])

    data_ingestion = DataIngestion()

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:
        readable_files = (p
                          | 'Matching .csv files' >> fileio.MatchFiles('gs://hc_chs_ats/File_Temp/Temp_File/*.csv')
                          | 'Read Matches' >> fileio.ReadMatches()
                          | 'Rebalance data inputs' >> beam.Reshuffle())
        files_and_content = (readable_files
                             | 'Determine FilePath' >> beam.Map(lambda x: x.metadata.path))
        writebq = (files_and_content
                   | 'Read from a File' >> beam.io.ReadAllFromText(skip_header_lines=1)
                   | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
                   | 'Write to BigQuery' >> beam.io.Write(
                    beam.io.WriteToBigQuery(known_args.output,
                                            schema='Requisition_Number:STRING,'
                                                   'Opportunity_Title:STRING,'
                                                   'Opportunity_Status:STRING,'
                                                   'Featured:STRING,'
                                                   'Company_Code:STRING,'
                                                   'Company:STRING,'
                                                   'Entity:STRING,'
                                                   'Entity_Desc:STRING,'
                                                   'Source_Job_Code:STRING,'
                                                   'Job_Title:STRING,'
                                                   'FullTime_Or_PartTime:STRING,'
                                                   'Salary_Or_Hourly:STRING,'
                                                   'Recruiter:STRING,'
                                                   'Location_Name:STRING,'
                                                   'Date_Applied:DATE,'
                                                   'Source:STRING,'
                                                   'Step:STRING,'
                                                   'Step_Date:DATE,'
                                                   'Recruiting_Hire_Date:DATE,'
                                                   'Start_Date:DATE,'
                                                   'Candidate:STRING,'
                                                   'Candidate_Email_Address:STRING,'
                                                   'Candidate_Primary_Phone:STRING,'
                                                   'First_Published_Date:DATE,'
                                                   'Average_Days_Between_Publish_Hire_Dates:STRING',
                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))


if __name__ == '__main__':
    #logging.getLogger().setLevel(logging.INFO)
    run()
