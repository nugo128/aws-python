import json
import os
import urllib
import uuid
import boto3


def start_video_label_detection(bucket, key):
  rekognition_client = boto3.client('rekognition')
  response = rekognition_client.start_label_detection(
    Video={'S3Object': {
      'Bucket': bucket,
      'Name': key
    }},
    NotificationChannel={
      'SNSTopicArn': os.environ['REKOGNITION_SNS_TOPIC_ARN'],
      'RoleArn': os.environ['REKOGNITION_ROLE_ARN']
    })
  print("Job For Rekognition Started")
  return


# https://docs.aws.amazon.com/rekognition/latest/dg/labels-detect-labels-image.html
def get_image_labels(bucket, key):
  rekognition_client = boto3.client('rekognition')
  response = rekognition_client.detect_labels(
    Image={'S3Object': {
      'Bucket': bucket,
      'Name': key
    }}, MaxLabels=10)

  return response


def get_video_labels(job_id):
  rekognition_client = boto3.client('rekognition')
  response = rekognition_client.get_label_detection(JobId=job_id)
  next_token = response.get('NextToken', None)

  while next_token:
    next_page = rekognition_client.get_label_detection(JobId=job_id,
                                                       NextToken=next_token)

    next_token = next_page.get('NextToken', None)

    response['Labels'].extend(next_page['Labels'])

  return response


def make_item(data):
  if isinstance(data, dict):
    return {k: make_item(v) for k, v in data.items()}

  if isinstance(data, list):
    return [make_item(v) for v in data]

  if isinstance(data, float):
    return str(data)

  return data


def put_labels_in_db(data, media_name, media_bucket):
  data.pop('ResponseMetadata', None)

  if 'JobStatus' in data:
    del data['JobStatus']
    data['mediaType'] = 'Video'
  else:
    data['mediaType'] = 'Image'

  data['mediaName'] = media_name
  data['mediaBucket'] = media_bucket

  data['id'] = str(uuid.uuid1())

  dynamodb = boto3.resource('dynamodb')
  table_name = os.environ['DYNAMO_DB_TABLE']
  videos_table = dynamodb.Table(table_name)

  data = make_item(data)

  videos_table.put_item(Item=data)

  return


# Lambda events


def start_processing_media(event, context):
  for record in event['Records']:
    objectExtenstion = record['s3']['object']['key'].split('.')[-1]
    # https://t.ly/C5T-
    if objectExtenstion in ['jpeg', 'png', 'mp4']:
      bucket_name = record['s3']['bucket']['name']
      object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
      if objectExtenstion == 'mp4':
        start_video_label_detection(bucket_name, object_key)
      else:
        put_labels_in_db(get_image_labels(bucket_name, object_key), object_key,
                         bucket_name)
  return


def handle_label_detection(event, context):
  for record in event['Records']:
    message = json.loads(record['Sns']['Message'])
    job_id = message['JobId']
    s3_object = message['Video']['S3ObjectName']
    s3_bucket = message['Video']['S3Bucket']

    response = get_video_labels(job_id)
    put_labels_in_db(response, s3_object, s3_bucket)

  return
