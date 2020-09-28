import requests
import gzip
from ftplib import FTP
from xml.etree import ElementTree
import io

from google.cloud import storage
import json
import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/alfonzosanfilippo/PycharmProjects/BQ_Projects/venv/Resource/hireclix.json'

def function():
    storage_client = storage.Client()
    data = storage_client.get_bucket('hc_tokens_scripts').blob('Tokens/GaloisFTP.json').download_as_string()
    json_data = json.loads(data)

    web_response = requests.get(
        "https://clickcastfeeds.s3.amazonaws.com/22608d86025fa9a05c8a9a1d7e95c6a7/2105aeb69fe9cf3475ae0be14aa06fd6.xml.gz")

    xml = gzip.decompress(web_response.content).decode()

    xmldata = io.BytesIO(xml.encode('utf-8'))

    ftp = FTP(host=json_data["host"]).login(user=json_data["username"], passwd=json_data["password"])
    
    ftp.storbinary('STOR UberJapanJobs.xml', xmldata)


function()