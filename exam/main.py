import argparse
import io
from urllib.request import urlopen
from botocore.exceptions import ClientError
from auth import init_client


def bucket_exists(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return True
    except ClientError:
        return False
    return False


def enable_versioning(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return True
    except ClientError:
        return False
    return False


def download_and_upload(aws_s3_client, bucket_name, key, url):
    with urlopen(url) as response:
        content = response.read()
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            ExtraArgs={"ContentType": content_type},
            Key=key,
        )

    region = aws_s3_client.meta.region_name
    if region == "us-east-1":
        return f"https://{bucket_name}.s3.amazonaws.com/{key}"
    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{key}"


def delete_object(aws_s3_client, bucket_name, key):
    try:
        response = aws_s3_client.delete_object(Bucket=bucket_name, Key=key)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 204:
            return True
    except ClientError:
        return False
    return False


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    user_parser = subparsers.add_parser("user")
    user_parser.add_argument("-n", "--name", required=True)
    user_parser.add_argument("-s", "--surname", required=True)

    bucket_parser = subparsers.add_parser("bucket")
    bucket_parser.add_argument("-n", "--name", required=True)
    bucket_parser.add_argument("-exists", "--exists", action="store_true")
    bucket_parser.add_argument("-ev", "--enable_versioning", choices=["True", "False"])

    object_parser = subparsers.add_parser("object")
    object_parser.add_argument("bucket_name")
    object_parser.add_argument("-on", "--object_name", required=True)
    object_parser.add_argument("-d", "--delete", action="store_true")
    object_parser.add_argument("-link", "--link")

    args = parser.parse_args()

    if args.command == "user":
        print(f"Good Luck, {args.name} {args.surname} !")

    elif args.command == "bucket":
        s3_client = init_client()
        if args.exists:
            print(f"Bucket exists: {bucket_exists(s3_client, args.name)}")
        if args.enable_versioning == "True":
            print(f"Versioning enabled: {enable_versioning(s3_client, args.name)}")

    elif args.command == "object":
        s3_client = init_client()
        if args.delete:
            print(f"Object deleted: {delete_object(s3_client, args.bucket_name, args.object_name)}")
        if args.link:
            print(download_and_upload(s3_client, args.bucket_name, args.object_name, args.link))


if __name__ == "__main__":
    main()
