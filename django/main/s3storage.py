import os

import boto3
from capd1.settings import AWS_ACCESS_KEY_ID, AWS_STORAGE_BUCKET_NAME, MEDIA_URL, SECRET_KEY

from main.models import Request


def get_s3_client():
    client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=SECRET_KEY)

    return client


def download_results(s3_url, save_as):
    try:
        if os.path.exists(f"{MEDIA_URL}{save_as}"):
            print("already exists")
            return
        client = get_s3_client()
        client.download_file(Bucket=AWS_STORAGE_BUCKET_NAME, Key=s3_url, Filename=f"{MEDIA_URL}{save_as}")
    except Exception as e:
        print(e.__str__())
        print("no data exists")


def get_files(data):
    if not os.path.exists(MEDIA_URL):
        os.mkdir(MEDIA_URL)

    if data.year and data.quarter:
        file_name = f"{data.year}_{data.quarter}_report.json"
        s3_url = f"result/new/{file_name}"
        download_results(s3_url, file_name)

        funcs = [1, 2, 3, 4, 5, 7, 8, 10, 11, 14, 15]
        for func in funcs:
            file_name = f"{func}_{data.year}_{data.quarter}_report.json"
            s3_url = f"logs/new/{file_name}"

            download_results(s3_url, file_name)
