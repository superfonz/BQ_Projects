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
import time

# Regular expression operations <https://docs.python.org/3/library/re.html>
import re

# Google Authentication <https://google-auth.readthedocs.io/en/latest/reference/google.auth.credentials.html>
import google.auth

# Basic date and time types <https://docs.python.org/2/library/datetime.html>
from datetime import datetime, timedelta

# Python Data Analysis Library <https://pandas.pydata.org/docs/user_guide/index.html>
import pandas as pd
import numpy as np

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
            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
            # <----------------->
            print("import")
            # Read and import .xlsx file and store in a dataframe
            dataframe = pd.read_excel('gs://hc_crackerbarrel_ats/File_Upload/' + match.group(1) + ".xlsx",
                                      sheet_name="Sheet1")
            print("finish")
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

            dataframe['Location_Store'] = dataframe['Location_Store'].astype('int64', errors='ignore')

            regionids = {33: 1, 428: 1, 223: 1, 691: 1, 521: 1, 53: 1, 83: 1, 96: 1, 423: 1, 614: 1, 360: 1, 311: 1, 55: 1, 66: 1, 113: 1, 131: 1, 195: 1, 220: 1, 338: 1, 493: 1, 517: 1, 382: 1, 9: 1, 21: 1, 75: 1, 79: 1, 546: 1, 573: 1, 610: 1, 216: 1, 415: 1, 443: 1, 522: 1, 542: 1, 632: 1, 48: 1, 101: 1, 153: 1, 290: 1, 303: 1, 417: 1, 650: 1, 10: 1, 19: 1, 71: 1, 77: 1, 355: 1, 651: 1, 698: 1, 751: 1, 6: 1, 16: 1, 18: 1, 170: 1, 494: 1, 514: 1, 586: 1, 3: 1, 11: 1, 15: 1, 90: 1, 134: 1, 429: 1, 530: 1, 565: 1, 4: 1, 8: 1, 12: 1, 358: 1, 454: 1, 483: 1, 549: 1, 556: 1, 706: 1, 2: 1, 7: 1, 13: 1, 23: 1, 26: 1, 34: 1, 86: 1, 87: 1, 470: 1, 630: 1, 112: 2, 249: 2, 330: 2, 578: 2, 607: 2, 625: 2, 663: 2, 60: 2, 128: 2, 263: 2, 281: 2, 463: 2, 568: 2, 582: 2, 72: 2, 184: 2, 559: 2, 629: 2, 677: 2, 658: 2, 652: 2, 108: 2, 81: 2, 309: 2, 495: 2, 560: 2, 606: 2, 570: 2, 69: 2, 76: 2, 100: 2, 107: 2, 461: 2, 564: 2, 688: 2, 717: 2, 58: 2, 194: 2, 313: 2, 459: 2, 478: 2, 566: 2, 678: 2, 708: 2, 142: 2, 247: 2, 269: 2, 340: 2, 672: 2, 693: 2, 695: 2, 271: 2, 343: 2, 345: 2, 363: 2, 539: 2, 551: 2, 675: 2, 106: 2, 207: 2, 248: 2, 329: 2, 502: 2, 592: 2, 659: 2, 39: 2, 41: 2, 47: 2, 160: 2, 356: 2, 448: 2, 642: 2, 118: 4, 157: 4, 158: 4, 289: 4, 364: 4, 524: 4, 647: 4, 63: 4, 67: 4, 91: 4, 267: 4, 250: 4, 499: 4, 519: 4, 520: 4, 44: 4, 201: 4, 265: 4, 477: 4, 479: 4, 557: 4, 749: 4, 35: 4, 54: 4, 89: 4, 341: 4, 361: 4, 511: 4, 577: 4, 589: 4, 744: 4, 240: 4, 241: 4, 268: 4, 287: 4, 380: 4, 500: 4, 545: 4, 56: 4, 82: 4, 167: 4, 481: 4, 498: 4, 535: 4, 626: 4, 73: 4, 117: 4, 119: 4, 225: 4, 282: 4, 506: 4, 527: 4, 189: 4, 357: 4, 488: 4, 489: 4, 532: 4, 603: 4, 649: 4, 151: 4, 190: 4, 251: 4, 503: 4, 529: 4, 534: 4, 547: 4, 291: 5, 411: 5, 442: 5, 484: 5, 504: 5, 509: 5, 598: 5, 299: 5, 312: 5, 386: 5, 435: 5, 449: 5, 512: 5, 743: 5, 238: 5, 239: 5, 262: 5, 333: 5, 537: 5, 373: 5, 412: 5, 452: 5, 462: 5, 497: 5, 665: 5, 298: 5, 302: 5, 306: 5, 387: 5, 410: 5, 699: 5, 342: 5, 472: 5, 485: 5, 555: 5, 572: 5, 608: 5, 687: 5, 718: 5, 65: 5, 126: 5, 193: 5, 336: 5, 348: 5, 422: 5, 510: 5, 199: 5, 374: 5, 431: 5, 482: 5, 550: 5, 681: 5, 696: 5, 164: 5, 258: 5, 280: 5, 434: 5, 605: 5, 704: 5, 731: 5, 293: 5, 294: 5, 317: 5, 318: 5, 350: 5, 406: 5, 436: 5, 84: 6, 176: 6, 196: 6, 235: 6, 307: 6, 369: 6, 437: 6, 125: 6, 129: 6, 147: 6, 209: 6, 221: 6, 246: 6, 391: 6, 120: 6, 122: 6, 143: 6, 175: 6, 214: 6, 244: 6, 127: 6, 140: 6, 145: 6, 179: 6, 188: 6, 389: 6, 590: 6, 720: 6, 163: 6, 177: 6, 243: 6, 366: 6, 392: 6, 762: 6, 767: 6, 36: 6, 168: 6, 308: 6, 427: 6, 465: 6, 722: 6, 724: 6, 174: 6, 181: 6, 213: 6, 351: 6, 458: 6, 78: 6, 93: 6, 111: 6, 232: 6, 404: 6, 536: 6, 569: 6, 161: 6, 191: 6, 219: 6, 296: 6, 365: 6, 444: 6, 445: 6, 447: 6, 99: 6, 148: 6, 150: 6, 279: 6, 533: 6, 567: 6, 144: 6, 319: 6, 325: 6, 332: 6, 24: 7, 372: 7, 420: 7, 453: 7, 460: 7, 513: 7, 531: 7, 745: 7, 51: 7, 97: 7, 217: 7, 475: 7, 476: 7, 612: 7, 664: 7, 682: 7, 123: 7, 272: 7, 301: 7, 315: 7, 368: 7, 396: 7, 599: 7, 139: 7, 229: 7, 274: 7, 278: 7, 304: 7, 316: 7, 395: 7, 433: 7, 275: 7, 276: 7, 347: 7, 457: 7, 468: 7, 634: 7, 94: 7, 109: 7, 264: 7, 326: 7, 438: 7, 439: 7, 761: 7, 17: 7, 92: 7, 284: 7, 394: 7, 544: 7, 639: 7, 716: 7, 80: 7, 231: 7, 384: 7, 455: 7, 523: 7, 70: 7, 237: 7, 273: 7, 558: 7, 574: 7, 580: 7, 712: 7, 730: 7, 138: 8, 152: 8, 182: 8, 352: 8, 353: 8, 686: 8, 210: 8, 252: 8, 253: 8, 266: 8, 594: 8, 609: 8, 633: 8, 637: 8, 203: 8, 205: 8, 242: 8, 322: 8, 327: 8, 583: 8, 166: 8, 173: 8, 206: 8, 653: 8, 676: 8, 692: 8, 710: 8, 766: 8, 226: 8, 227: 8, 270: 8, 292: 8, 346: 8, 359: 8, 683: 8, 197: 8, 375: 8, 381: 8, 414: 8, 430: 8, 467: 8, 719: 8, 769: 8, 154: 8, 211: 8, 228: 8, 421: 8, 576: 8, 595: 8, 684: 8, 690: 8, 149: 8, 198: 8, 377: 8, 600: 8, 623: 8, 714: 8, 631: 8, 202: 8, 321: 8, 645: 8, 668: 8, 670: 8, 673: 8, 37: 9, 45: 9, 88: 9, 95: 9, 400: 9, 440: 9, 554: 9, 85: 9, 121: 9, 183: 9, 224: 9, 310: 9, 466: 9, 490: 9, 114: 9, 215: 9, 383: 9, 385: 9, 418: 9, 525: 9, 180: 9, 208: 9, 218: 9, 234: 9, 236: 9, 464: 9, 538: 9, 61: 9, 169: 9, 295: 9, 285: 9, 492: 9, 507: 9, 553: 9, 105: 9, 370: 9, 376: 9, 441: 9, 474: 9, 480: 9, 640: 9, 49: 9, 62: 9, 74: 9, 541: 9, 526: 9, 393: 9, 156: 9, 694: 9, 68: 9, 104: 9, 115: 9, 133: 9, 136: 9, 222: 9, 528: 9, 64: 9, 116: 9, 132: 9, 256: 9, 487: 10, 591: 10, 705: 10, 185: 10, 261: 10, 300: 10, 371: 10, 747: 10, 508: 10, 25: 10, 124: 10, 141: 10, 283: 10, 581: 10, 750: 10, 667: 10, 628: 10, 32: 10, 146: 10, 171: 10, 657: 10, 593: 10, 505: 10, 5: 10, 29: 10, 102: 10, 200: 10, 424: 10, 471: 10, 562: 10, 42: 10, 446: 10, 491: 10, 579: 10, 635: 10, 689: 10, 52: 10, 192: 10, 328: 10, 680: 10, 685: 10, 419: 10, 38: 10, 40: 10, 212: 10, 543: 10, 561: 10, 700: 10, 613: 10, 22: 10, 98: 10, 46: 10, 401: 10, 456: 10, 515: 10, 516: 10, 28: 10, 339: 10, 473: 10, 588: 10, 611: 10, 641: 10, 746: 10, 20: 10, 30: 10, 59: 10, 425: 10, 584: 10, 697: 10, 172: 11, 288: 11, 320: 11, 379: 11, 451: 11, 622: 11, 679: 11, 245: 11, 277: 11, 305: 11, 335: 11, 344: 11, 388: 11, 617: 11, 619: 11, 314: 11, 323: 11, 337: 11, 602: 11, 741: 11, 742: 11, 297: 11, 331: 11, 334: 11, 362: 11, 416: 11, 615: 11, 618: 11, 655: 11, 735: 11, 407: 11, 729: 11, 733: 11, 736: 11, 737: 11, 759: 11, 753: 11, 754: 11, 755: 11, 765: 11, 756: 11}
            skillpositionid = {"server": "Server", "cook": "Cook", "dishwasher": "Dishwasher", "Delivery Driver": "Distribution Center", "Material Handler": "Distribution Center", "fast track": "General Manager", "kitchen": "Kitchen Manager", "night": "Night Maintenance", "Restaurant Manager": "Restaurant Manager", "import": "Home Office", "rivs": "Home Office", "supply": "Home Office", "acqu": "Home Office", "systems": "Home Office", "test d": "Home Office", "store o": "Home Office", "staff au": "Home Office", "account": "Home Office", "district": "Home Office", "Retail Bench Manager": "Home Office", "lead": "Home Office", "vice": "Home Office", "loss": "Home Office", "order": "Home Office", "operation": "Home Office", "Merchandise": "Home Office", "strate": "Home Office", "plan": "Home Office", "exp": "Home Office", "culinary": "Home Office", "consumer": "Home Office", "execu": "Home Office", "opera": "Home Office", "architect": "Home Office", "associate": "Home Office", "catering": "Home Office", "analyst": "Home Office", "corporate": "Home Office", "intern": "Home Office", "director": "Home Office", "diversity": "Home Office", "employee": "Home Office", "internal": "Home Office", "Facilit": "Home Office", "financ": "Home Office", "fixed": "Home Office", "specialist": "Home Office", "housekeeping": "Home Office", "coordin": "Home Office", "tech": "Home Office", "sr": "Home Office", "Host": "Host", "Retail Sales": "Retail Sales", "Retail Manager": "Retail Manager"}

            dataframe['Region'] = dataframe['Location_Store'].map(regionids)
            dataframe['Region'] = dataframe['Region'].astype(str).replace(r'\.0', '', regex=True)

            dataframe['minifiedReqID'] = dataframe['Job_RequisitionID'].replace(to_replace=r'^.*-', value='', regex=True,)
            dataframe['SkillPosition'] = dataframe['Job_PositionTitle']

            for key in skillpositionid:
                dataframe['SkillPosition'] = dataframe['SkillPosition'].replace(to_replace=r'^.*(?i)' + key + '.*$', value=skillpositionid[key], regex=True)

            dataframe['CityState'] = dataframe['Location_City'] + "-" + dataframe['Location_StateProvince']

            # Convert Numeric Objects into Strings - Improper Data Type
            dataframe['Job_RequisitionID'] = dataframe['Job_RequisitionID'].astype('str')
            # <--------------------->

            # String to Query BigQuery Table / Uses SQL Format -
            # Below Basically Means Everything from the Specific Table
            #dataframe.to_csv("test.csv", index=False)
            print(dataframe)

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
                print("retrive")
                # Request data from BigQuery
                bgdata = (
                    bigquery_client.query(query_string)
                        .result()
                        .to_dataframe(bqstorage_client=biqquery_storage_client)  # convert to dataframe
                )
                print("dedup")
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
            print("send to bq")
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
                                            {'name': 'Job_RequisitionID', 'type': 'STRING'},
                                            {'name': 'Region', 'type': 'STRING'},
                                            {'name': 'minifiedReqID', 'type': 'STRING'},
                                            {'name': 'SkillPosition', 'type': 'STRING'},
                                            {'name': 'CityState', 'type': 'STRING'}])

            # export dataframe to File_Backup/ folder in the datastore, with today's date
            print("f gcs")
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
if __name__ == "__main__":
    start_time = time.time()
    CB_ATS_Data()
    print("--- %s seconds ---" % (time.time() - start_time))




