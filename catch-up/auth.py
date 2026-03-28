import boto3
from os import getenv
from dotenv import load_dotenv

load_dotenv()

def init_client(region_name=None):
    if region_name:
        # If a region is provided, use it.
        aws_region = region_name
    else:
        # Otherwise, fall back to the environment variable.
        aws_region = getenv("aws_region_name")

    client = boto3.client(
        "s3",
        aws_access_key_id=getenv("aws_access_key_id"),
        aws_secret_access_key=getenv("aws_secret_access_key"),
        aws_session_token=getenv("aws_session_token"),
        region_name=aws_region,
    )
    # Check if credentials are correct
    client.list_buckets()

    return client
