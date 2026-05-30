"""AWS Lambda: analyse newly uploaded S3 images with Rekognition.

Triggered by S3 ObjectCreated events for .jpg/.jpeg/.png objects (the myauto
images uploaded by cli.py). For each image it calls Rekognition DetectLabels
and stores the result in the DynamoDB table `rekogintionAnalysesDB`.

Adapted from the lecture_6 rekognition handler; trimmed to the image path and
pointed at the rekogintionAnalysesDB table.
"""

import os
import urllib.parse
import uuid

import boto3

# Extensions we hand to Rekognition. NOTE: myauto images are `.jpg`, so it is
# included here in addition to the lecture's jpeg/png.
IMAGE_EXTENSIONS = ("jpg", "jpeg", "png")


def get_image_labels(bucket, key):
    """Ask Rekognition for up to 10 labels describing the S3 image."""
    rekognition = boto3.client("rekognition")
    # https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html
    return rekognition.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MaxLabels=10,
    )


def make_item(data):
    """DynamoDB cannot store floats, so recursively convert them to strings."""
    if isinstance(data, dict):
        return {k: make_item(v) for k, v in data.items()}
    if isinstance(data, list):
        return [make_item(v) for v in data]
    if isinstance(data, float):
        return str(data)
    return data


def put_labels_in_db(data, media_name, media_bucket):
    """Persist a Rekognition response into the rekogintionAnalysesDB table."""
    data.pop("ResponseMetadata", None)

    data["mediaType"] = "Image"
    data["mediaName"] = media_name
    data["mediaBucket"] = media_bucket
    data["id"] = str(uuid.uuid1())

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(os.environ["DYNAMO_DB_TABLE"])
    table.put_item(Item=make_item(data))


def start_processing_media(event, context):
    """S3 ObjectCreated entry point."""
    for record in event["Records"]:
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        extension = key.rsplit(".", 1)[-1].lower()
        if extension not in IMAGE_EXTENSIONS:
            continue
        bucket = record["s3"]["bucket"]["name"]
        print(f"Analysing s3://{bucket}/{key} with Rekognition...")
        labels = get_image_labels(bucket, key)
        put_labels_in_db(labels, key, bucket)
        names = [l["Name"] for l in labels.get("Labels", [])]
        print(f"  stored labels for {key}: {names}")
    return {"status": "ok"}
