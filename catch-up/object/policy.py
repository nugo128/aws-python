def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    response = aws_s3_client.put_object_acl(
        ACL="public-read", Bucket=bucket_name, Key=file_name
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def set_lifecycle_policy(aws_s3_client, bucket_name, days=120):
    lifecycle_config = {
        "Rules": [
            {
                "ID": f"DeleteAfter{days}Days",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
                "Expiration": {"Days": days},
            }
        ]
    }
    response = aws_s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_config,
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        print(f"Lifecycle policy set: objects will be deleted after {days} days")
        return True
    return False
