import requests
import logging
from string import Template
import datetime
import os
import config
import pandas as pd
from google.cloud import storage


def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    start_date, end_date = get_first_last_date_of_previous_month()
    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            path, filename = download_data(start_date, end_date, "Episode Date", ["Assigned_ID"])
            upload_blob(config.config_vars["bucket_name"], path, filename)
        except Exception as error:
            log_message = Template('Query failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('Query failed due to '
                                   '$message.')
        logging.error(log_message.safe_substitute(message=error))


def download_data(
        start_date: str, 
        end_date: str,
        date_col_name: str,
        col_to_drop: list = None,
    ) -> tuple[str]:
    """Calls API and downloads data to /tmp/ as a csv.
    Args:
        start_date: Start date of data
        end_date: End date of data
        date_col_name: The header of the date column used to slice the data
        col_to_drop: columns to drop
    """
    url = config.config_vars["api_url"]
    params = config.config_vars["api_params"]
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
    except KeyError as error:
        msg = "Could not find column: '{}'".format(error.args[0])
        logging.error(msg)
        raise KeyError(msg)
        
    filename = "{}_{}_{}.csv".format(config.config_vars["filename_prefix"], start_date, end_date)
    path = "/tmp/" + filename
    filtered_df.to_csv(path, index=False, header=False)
    print("Successfully uploaded to {}".format(path))

    return (path, filename)


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket and deletes from /tmp/."""
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

    try:
        os.remove(source_file_name)
    except OSError as error:
        print("Error: %s : %s" % (source_file_name, error.strerror))

def get_first_last_date_of_previous_month() -> tuple[str]:
    """Return first and last date of the previous month."""
    last_date = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
    first_date = datetime.date(last_date.year, last_date.month, 1)
    return (first_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"))

if __name__ == "__main__":
    main('data', 'context')