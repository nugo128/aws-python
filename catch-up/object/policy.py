def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    response = aws_s3_client.put_object_acl(
        ACL="public-read", Bucket=bucket_name, Key=file_name
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False
