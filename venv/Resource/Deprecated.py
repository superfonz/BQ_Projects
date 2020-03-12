
def move_file():
    client = storage.Client()
    for blob in client.list_blobs("hc_crackerbarrel_ats", prefix="File_Upload/"):
        f= re.search("/(.+?).csv",str(blob))
        if f:
            return f.group(1) + ".csv"

def CopyFile(SourceBucket,SourceBlob,DestBucket,DestBlob):
    client = storage.Client()
    source_bucket = client.get_bucket(SourceBucket)
    source_blob = source_bucket.blob(SourceBlob)
    destination_bucket = client.get_bucket(DestBucket)
    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, DestBlob)
    source_blob.delete()

def move_file():
    client = storage.Client()
    for blob in client.list_blobs("hc_crackerbarrel_ats", prefix="File_Upload/"):
        f = re.search("/(.+?).csv", str(blob))
        if f:
            found = f.group(1) + ".csv"
            source_bucket = client.get_bucket("hc_crackerbarrel_ats")
            source_blob = source_bucket.blob("File_Upload/" + found)
            destination_bucket = client.get_bucket("hc_crackerbarrel_ats")
            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, "File_Completed/" + found)
            source_blob.delete()

def findxlsx():
    client = storage.Client()
    for blob in client.list_blobs("hc_crackerbarrel_ats", prefix="File_Upload/"):
        f = re.search("/(.+?).xlsx", str(blob))
        if f:
            print('found excel file')
            return True

    print('didn\'t find excel file')
    return False
