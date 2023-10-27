import boto3
import json

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        source_bucket = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        target_bucket = 'copy-of-raw-json-bucket1'
        copy_source = {'Bucket': source_bucket, 'Key': object_key}

        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=object_key)
        s3_client.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)

    return {
        'statusCode': 200,
        'body': json.dumps('Copy completed successfully for all files')
    }
