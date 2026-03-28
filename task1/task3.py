import argparse
import boto3
import logging
from botocore.exceptions import ClientError
from os import getenv
from dotenv import load_dotenv

load_dotenv()

def init_client():
    """AWS S3 კლიენტის ინიციალიზაცია .env ფაილიდან."""
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

def bucket_exists(aws_s3_client, bucket_name):
    """ამოწმებს, არსებობს თუ არა მოთხოვნილი ბაკეტი."""
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return True
    except ClientError as e:
        return False
    return False

def delete_bucket(aws_s3_client, bucket_name):
    """შლის მითითებულ ბაკეტს."""
    try:
        aws_s3_client.delete_bucket(Bucket=bucket_name) 
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketNotEmpty':
            print(f"შეცდომა: ბაკეტი '{bucket_name}' არ არის ცარიელი. წასაშლელად ჯერ მასში არსებული ფაილები უნდა წაშალოთ.")
        else:
            logging.error(e)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ამოწმებს და შლის S3 ბაკეტს.")
    parser.add_argument("bucket_name", help="S3 ბაკეტის სახელი წასაშლელად.")
    
    args = parser.parse_args()
    s3_client = init_client()

    if s3_client:
        print(f"ვამოწმებ ბაკეტს: '{args.bucket_name}'...")
        
        if bucket_exists(s3_client, args.bucket_name):
            print(f"ბაკეტი '{args.bucket_name}' არსებობს. ვიწყებ მის წაშლას...")
            
            if delete_bucket(s3_client, args.bucket_name):
                print(f"ბაკეტი '{args.bucket_name}' წარმატებით წაიშალა!")
        else:
            print(f"ბაკეტი '{args.bucket_name}' არ არსებობს.")