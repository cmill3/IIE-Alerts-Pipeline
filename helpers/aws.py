import pandas as pd
import boto3

def get_s3_client():
    return boto3.client('s3')

def pull_files_s3(s3, bucket, key):
    """
    Pulls files from S3 bucket
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    data = pd.read_csv(response.get("Body"))
    return data['Unnamed: 0'].values[1:]