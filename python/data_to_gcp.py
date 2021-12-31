import requests
import logging

import pandas as pd
from google.cloud import storage


URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show"
PARAMS = {"id": "64b54586-6180-4485-83eb-81e8fae3b8fe"}
BUCKET_NAME = "bucket_name"
FILENAME_PREFIX = "prefix"
DIRECTORY = ""
start_date = "2021-12-01"
end_date = "2021-12-31"

def run(
    url: str, 
    params: dict, 
    bucket: str, 
    start_date: str, 
    end_date: str,
    date_col_name: str,
    col_to_drop: list = None,
    onetime: bool = False,
):
    package = requests.get(url, params=params).json()
    csv_url = package["result"]["resources"][0]["url"]
    df_list = []
    try:
        for chunk in pd.read_csv(csv_url, chunksize=10000):
            df = chunk.loc[(chunk[date_col_name] <= end_date) & (chunk[date_col_name] >= start_date)]
            if not df.empty:
                df_list.append(df)
        filtered_df = pd.concat(df_list, ignore_index=True)
        filtered_df.drop(columns=col_to_drop, inplace=True)
        print(filtered_df)
    except KeyError as e:
        logging.error("Could not find column: '{}'".format(e.args[0]))
        
    filename = "{}_{}_{}.csv".format(FILENAME_PREFIX, start_date, end_date)
    path = DIRECTORY + filename
    filtered_df.to_csv(path, index=False, header=False)
    upload_blob(bucket, path, filename)
    
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


if __name__ == "__main__":
    run(
        URL, 
        PARAMS, 
        BUCKET_NAME, 
        start_date, 
        end_date, 
        "Episode Date", 
        ["Assigned_ID"]
        )