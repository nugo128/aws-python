from urllib.request import urlopen
import os
import io
import mimetypes
from hashlib import md5
from time import localtime
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig


def get_objects(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.list_objects(Bucket=bucket_name)
        if "Contents" in response:
            for key in response["Contents"]:
                print(f"  {key['Key']}, size: {key['Size']}")
        else:
            print("Bucket is empty or access to list objects is denied.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AccessDenied':
            print("Access Denied: You do not have the s3:ListBucket permission.")
        else:
            print(f"An AWS error occurred: {e}")


def download_file_and_upload_to_s3(
    aws_s3_client, bucket_name, url, keep_local=False
) -> str:
    file_name = f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}.jpg'
    with urlopen(url) as response:
        content = response.read()
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            ExtraArgs={"ContentType": "image/jpg"},
            Key=file_name,
        )
    if keep_local:
        with open(file_name, mode="wb") as jpg_file:
            jpg_file.write(content)

    # public URL
    region = aws_s3_client.meta.region_name
    if region == "us-east-1":
        return f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
    else:
        return f"https://{bucket_name}.s3.{region}.amazonaws.com/{file_name}"


def validate_mimetype(filename, allowed_types=None):
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type is None:
        mime_type = "application/octet-stream"
    if allowed_types and mime_type not in allowed_types:
        raise ValueError(
            f"File type '{mime_type}' is not allowed. Allowed types: {allowed_types}"
        )
    return mime_type


def upload_file(aws_s3_client, filename, bucket_name, validate_mime=False):
    key = os.path.basename(filename)
    extra_args = {}
    if validate_mime:
        mime_type = validate_mimetype(filename)
        extra_args["ContentType"] = mime_type
        print(f"Detected MIME type: {mime_type}")
    aws_s3_client.upload_file(filename, bucket_name, key, ExtraArgs=extra_args or None)
    print(f"Uploaded '{filename}' as '{key}' to bucket '{bucket_name}'")
    return True


def upload_file_multipart(aws_s3_client, filename, bucket_name, validate_mime=False):
    key = os.path.basename(filename)
    file_size = os.path.getsize(filename)

    extra_args = {}
    if validate_mime:
        mime_type = validate_mimetype(filename)
        extra_args["ContentType"] = mime_type
        print(f"Detected MIME type: {mime_type}")

    # Multipart config: 25MB chunk size, 10 concurrent threads
    config = TransferConfig(
        multipart_threshold=25 * 1024 * 1024,
        multipart_chunksize=25 * 1024 * 1024,
        max_concurrency=10,
    )

    print(f"Uploading '{filename}' ({file_size} bytes) using multipart upload...")
    aws_s3_client.upload_file(
        filename, bucket_name, key,
        ExtraArgs=extra_args or None,
        Config=config,
    )
    print(f"Uploaded '{filename}' as '{key}' to bucket '{bucket_name}'")
    return True


def delete_object(aws_s3_client, bucket_name, key):
    response = aws_s3_client.delete_object(Bucket=bucket_name, Key=key)
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 204:
        print(f"Deleted '{key}' from bucket '{bucket_name}'")
        return True
    return False


def get_versioning_status(aws_s3_client, bucket_name):
    response = aws_s3_client.get_bucket_versioning(Bucket=bucket_name)
    status = response.get("Status", "Disabled")
    print(f"Versioning for '{bucket_name}': {status}")
    return status


def list_object_versions(aws_s3_client, bucket_name, key):
    response = aws_s3_client.list_object_versions(Bucket=bucket_name, Prefix=key)
    versions = [v for v in response.get("Versions", []) if v["Key"] == key]
    if not versions:
        print(f"No versions found for '{key}' in bucket '{bucket_name}'")
        return []
    print(f"Found {len(versions)} version(s) for '{key}':")
    for v in versions:
        print(f"  VersionId: {v['VersionId']}, LastModified: {v['LastModified']}, IsLatest: {v['IsLatest']}")
    return versions


def restore_previous_version(aws_s3_client, bucket_name, key):
    versions = list_object_versions(aws_s3_client, bucket_name, key)
    if len(versions) < 2:
        print("No previous version available to restore.")
        return False
    previous_version = versions[1]
    prev_id = previous_version["VersionId"]
    print(f"Restoring version '{prev_id}' as new latest version...")
    response = aws_s3_client.get_object(Bucket=bucket_name, Key=key, VersionId=prev_id)
    aws_s3_client.put_object(Bucket=bucket_name, Key=key, Body=response["Body"].read())
    print(f"Restored '{key}' to previous version (VersionId: {prev_id})")
    return True


def upload_file_obj(aws_s3_client, filename, bucket_name):
    with open(filename, "rb") as file:
        aws_s3_client.upload_fileobj(file, bucket_name, "hello_obj.txt")


def upload_file_put(aws_s3_client, filename, bucket_name):
    with open(filename, "rb") as file:
        aws_s3_client.put_object(
            Bucket=bucket_name, Key="hello_put.txt", Body=file.read()
        )
