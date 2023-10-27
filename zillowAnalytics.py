from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta, datetime
import json
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")
s3_bucket = 'cleaned-data-csv-bucket'

def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']

    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    output_file_path = f"/home/ubuntu/response_data_{querystring['location'].replace(', ', '_').replace(' ', '_')}_{dt_string}.json"
    file_str = f"response_data_{querystring['location'].replace(', ', '_').replace(' ', '_')}_{dt_string}.csv"

    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)
    output_list = [output_file_path, file_str]
    return output_list

default_args = {
    'owner': 'Danish',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 22),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

locations = ["seattle, wa", "san francisco, ca", "chicago, il", "new york, ny", "san diego, ca"]

with DAG('zillow_analytics_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start_task = DummyOperator(task_id="start")

    prev_task = start_task

    for location in locations:
        task_id_prefix = location.replace(", ", "_").replace(" ", "_").lower()

        wait_task = TimeDeltaSensor(
            task_id=f'wait_{task_id_prefix}',
            delta=timedelta(seconds=2),
        )

        extract_task = PythonOperator(
            task_id=f'tsk_extract_{task_id_prefix}',
            python_callable=extract_zillow_data,
            op_kwargs={
                'url': 'https://zillow56.p.rapidapi.com/search',
                'querystring': {"location": location},
                'headers': api_host_key,
                'date_string': dt_now_string
            }
        )

        load_task = BashOperator(
            task_id=f'tsk_load_to_s3_{task_id_prefix}',
            bash_command=f"""
            file="{{{{ ti.xcom_pull(task_ids='tsk_extract_{task_id_prefix}')[0] }}}}"
            echo "Moving file: $file"
            aws s3 mv "$file" s3://zillowproject-bucket/
            """,
            params={'location': location}
        )

        is_file_in_s3_available = S3KeySensor(
            task_id=f'task_file_in_s3_available_{task_id_prefix}',
            bucket_key=f'{{{{ti.xcom_pull("tsk_extract_{task_id_prefix}")[1]}}}}',
            bucket_name=s3_bucket,
            aws_conn_id='aws_s3_conn',
            wildcard_match=False,
            timeout=60,
            poke_interval=5,
        )

        transfer_s3_to_redshift = S3ToRedshiftOperator(
            task_id=f"task_transfer_s3_to_redshift_{task_id_prefix}",
            aws_conn_id='aws_s3_conn',
            redshift_conn_id='conn_id_redshift',
            s3_bucket=s3_bucket,
            s3_key=f'{{{{ti.xcom_pull("tsk_extract_{task_id_prefix}")[1]}}}}',
            schema="PUBLIC",
            table="zillowdata",
            copy_options=["csv IGNOREHEADER 1"],
        )

        prev_task >> wait_task >> extract_task >> load_task >> is_file_in_s3_available >> transfer_s3_to_redshift
        prev_task = extract_task
