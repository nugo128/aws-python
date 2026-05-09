import json
import os
import uuid
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

DYNAMODB_TABLE = os.environ["DYNAMO_DB_TABLE"]


def lambda_handler(event, context):
    print(event)
    data = json.loads(event["body"])
    print(json.dumps(data))

    try:
        save_to_dynamodb(data)
    except ClientError as e:
        print(e.response["Error"]["Message"])
    else:
        print("Data received Successfully")

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": "Data received Successfully",
    }


def save_to_dynamodb(data):
    dynamodb = boto3.client("dynamodb")
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    item = {
        "id": {"S": str(uuid.uuid1())},
        "name": {"S": data["name"]},
        "email": {"S": data["email"]},
        "phone": {"S": data["phone"]},
        "address": {"S": data["address"]},
        "programming_languages": {"S": data["programming_languages"]},
        "tools": {"S": data["tools"]},
        "createdAt": {"S": timestamp},
        "updatedAt": {"S": timestamp},
    }
    try:
        dynamodb.put_item(TableName=DYNAMODB_TABLE, Item=item)
    except ClientError as e:
        print(e.response["Error"]["Message"])
        raise e
    return
