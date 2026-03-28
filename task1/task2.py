import argparse
import boto3
import logging
import json
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

def check_policy(s3_client, bucket_name):
    try:
        s3_client.get_bucket_policy(Bucket=bucket_name)
        return True 
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
            return False
        else:
            print(f"შეცდომა Policy-ის შემოწმებისას: {e}")
            return False

def disable_public_access_block(s3_client, bucket_name):
    try:
        s3_client.delete_public_access_block(Bucket=bucket_name)
        print("Block Public Access წარმატებით მოიხსნა.")
    except ClientError as e:
        print(f"გაფრთხილება: ვერ მოხერხდა Public Access-ის ბლოკის მოხსნა: {e}")

def create_custom_policy(s3_client, bucket_name):
    
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadForDevAndTest",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}/dev/*",
                    f"arn:aws:s3:::{bucket_name}/test/*"
                ]
            }
        ]
    }
    
    disable_public_access_block(s3_client, bucket_name)
    
    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(policy) 
        )
        print(f"Policy წარმატებით შეიქმნა! '{bucket_name}'-ის /dev და /test ფოლდერები ახლა საჯაროა.")
    except ClientError as e:
        print(f"შეცდომა Policy-ის შექმნისას: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ამოწმებს და ქმნის Bucket Policy-ს.")
    parser.add_argument("bucket_name", help="S3 ბაკეტის სახელი.")
    args = parser.parse_args()
    
    s3_client = init_client()
    
    if s3_client:
        print(f"ვამოწმებ ბაკეტს: '{args.bucket_name}'...")
        
        if check_policy(s3_client, args.bucket_name):
            print("Policy უკვე არსებობს.")
        else:
            print("Policy არ არსებობს. ვიწყებ შექმნას...")
            create_custom_policy(s3_client, args.bucket_name)