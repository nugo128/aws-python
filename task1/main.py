import argparse
import boto3
import logging
from botocore.exceptions import ClientError
from os import getenv
from dotenv import load_dotenv

load_dotenv()

def init_client():
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=getenv("aws_access_key_id"),
            aws_secret_access_key=getenv("aws_secret_access_key"),
            aws_session_token=getenv("aws_session_token"),
            region_name=getenv("aws_region_name")
        )
        return client
    except ClientError as e:
        logging.error(e)
    except Exception as e:
        logging.error("Unexpected error")

def bucket_exists(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return True
    except ClientError as e:
        return False
    return False

def create_bucket(aws_s3_client, bucket_name, region='us-east-1'):
    try:
        if region == 'us-east-1':
            aws_s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            aws_s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
        return True
    except ClientError as e:
        logging.error(e)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ამოწმებს S3 ბაკეტის არსებობას და ქმნის მას საჭიროების შემთხვევაში.")
    parser.add_argument("bucket_name", help="S3 ბაკეტის სახელი, რომლის შემოწმება/შექმნაც გსურთ.")
    parser.add_argument("--region", default="us-east-1", help="AWS რეგიონი (ნაგულისხმევი: us-east-1)")
    
    args = parser.parse_args()
    s3_client = init_client()

    if s3_client:
        if bucket_exists(s3_client, args.bucket_name):
            print(f"Bucket '{args.bucket_name}' უკვე არსებობს.")
        else:
            print(f"Bucket '{args.bucket_name}' არ არსებობს. ვიწყებ შექმნას...")
            is_created = create_bucket(s3_client, args.bucket_name, args.region)
            if is_created:
                print(f"Bucket '{args.bucket_name}' წარმატებით შეიქმნა.")
            else:
                print(f"Bucket '{args.bucket_name}' შექმნა ვერ მოხერხდა.")