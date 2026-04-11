import json


def public_read_policy(bucket_name):
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            }
        ],
    }

    return json.dumps(policy)


def multiple_policy(bucket_name):
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:GetObjectAcl",
                    "s3:DeleteObject",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
                "Effect": "Allow",
                "Principal": "*",
            }
        ],
    }

    return json.dumps(policy)


def assign_policy(aws_s3_client, policy_function, bucket_name):
    policy = None
    if policy_function == "public_read_policy":
        policy = public_read_policy(bucket_name)
    elif policy_function == "multiple_policy":
        policy = multiple_policy(bucket_name)

    if not policy:
        print("please provide policy")
        return

    aws_s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy)
    print("Bucket multiple policy assigned successfully")


def read_bucket_policy(aws_s3_client, bucket_name):
    policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)

    status_code = policy["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return policy["Policy"]
    return False

def disable_public_access_block(aws_s3_client, bucket_name):
    aws_s3_client.delete_public_access_block(Bucket=bucket_name)
    print("Public access block disabled")


def enable_static_website(aws_s3_client, bucket_name, index_document="index.html", error_document="index.html"):
    aws_s3_client.put_bucket_website(
        Bucket=bucket_name,
        WebsiteConfiguration={
            "IndexDocument": {"Suffix": index_document},
            "ErrorDocument": {"Key": error_document},
        },
    )
    region = aws_s3_client.meta.region_name
    endpoint = f"http://{bucket_name}.s3-website-{region}.amazonaws.com"
    print(f"Static website hosting enabled for '{bucket_name}'")
    print(f"Endpoint: {endpoint}")
    return endpoint
