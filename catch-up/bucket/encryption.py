def set_bucket_encryption(aws_s3_client, bucket_name):
    response = aws_s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [
                {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
            ]
        },
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def read_bucket_encryption(aws_s3_client, bucket_name):
    return aws_s3_client.get_bucket_encryption(Bucket=bucket_name)
