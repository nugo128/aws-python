import json
import os
import urllib.error
import urllib.parse
import urllib.request

import boto3

s3 = boto3.client("s3")

HF_TOKEN = os.environ["HF_TOKEN"]
HF_API_URL = "https://router.huggingface.co/hf-inference/models/"

MODELS = {
    "mobilenet": "google/mobilenet_v1_0.75_192",
    "resnet": "microsoft/resnet-50",
    "mit": "nvidia/mit-b0",
    "yolos": "hustvl/yolos-tiny",
}

IMAGE_SUFFIXES = (".jpg", ".jpeg", ".png")


def query_hf(model_id, image_bytes):
    req = urllib.request.Request(
        HF_API_URL + model_id,
        data=image_bytes,
        headers={
            "Authorization": f"Bearer {HF_TOKEN}",
            "Content-Type": "application/octet-stream",
            "x-wait-for-model": "true",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def process_image(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    image_bytes = obj["Body"].read()

    image_name = os.path.splitext(os.path.basename(key))[0]

    for prefix, model_id in MODELS.items():
        try:
            result = query_hf(model_id, image_bytes)
        except urllib.error.HTTPError as e:
            result = {
                "model": model_id,
                "status": e.code,
                "error": e.read().decode("utf-8", errors="replace"),
            }
        except Exception as e:
            result = {"model": model_id, "error": str(e)}

        out_key = f"json/{prefix}_{image_name}.json"
        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=json.dumps(result, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"Saved s3://{bucket}/{out_key}")


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        if key.startswith("json/"):
            continue
        if not key.lower().endswith(IMAGE_SUFFIXES):
            continue

        process_image(bucket, key)

    return {"statusCode": 200}
