import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            source_bucket = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']

            target_bucket = 'cleaned-data-csv-bucket'
            target_file_name = object_key[:-5]

            waiter = s3_client.get_waiter('object_exists')
            waiter.wait(Bucket=source_bucket, Key=object_key)

            response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
            data = response['Body'].read().decode('utf-8')
            data = json.loads(data)

            f = []
            for i in data["results"]:
                f.append(i)

            df = pd.DataFrame(f)
            selected_columns = ['bathrooms', 'bedrooms', 'city', 'homeStatus', 
                                'homeType','livingArea','price', 'rentZestimate','zipcode']
            df = df[selected_columns]

            csv_data = df.to_csv(index=False)

            object_key_csv = f"{target_file_name}.csv"
            s3_client.put_object(Bucket=target_bucket, Key=object_key_csv, Body=csv_data)

        except Exception as e:
            print(f"Error processing file {object_key}: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('CSV conversion and S3 upload completed successfully for all files')
    }
