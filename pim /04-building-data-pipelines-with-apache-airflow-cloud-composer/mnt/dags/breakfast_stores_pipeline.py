from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account

import json


def _load_stores_to_gcs():
    DAGS_FOLDER = "/opt/airflow/dags/"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs = "/opt/airflow/dags/aerobic-ward-410501-51bb0361ca0a.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    project_id = "aerobic-ward-410501"

    # Load data from Local to GCS
    bucket_name = "my_workshop_007"
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    data = "stores"
    # file_path = f"{DATA_FOLDER}/{data}.csv"
    file_path = f"{DAGS_FOLDER}/breakfast_stores.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_stores_from_gcs_to_bigquery():
    DAGS_FOLDER = "/opt/airflow/dags/"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"

    bucket_name = "my_workshop_007"
    data = "stores"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    keyfile_bigquery = "/opt/airflow/dags/aerobic-ward-410501-51bb0361ca0a.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    project_id = "aerobic-ward-410501"

    # # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )
    table_id = f"{project_id}.breakfast.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()


with DAG(
    "breakfast_store_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 2, 4),
    tags=["breakfast", "PIM"],
):
    start = EmptyOperator(task_id="start")

    load_stores_to_gcs = PythonOperator(
        task_id="load_stores_to_gcs",
        python_callable=_load_stores_to_gcs,
    )

    load_stores_from_gcs_to_bigquery = PythonOperator(
        task_id="load_stores_from_gcs_to_bigquery",
        python_callable=_load_stores_from_gcs_to_bigquery,
    )

    end = EmptyOperator(task_id="end")

    start >> load_stores_to_gcs >> load_stores_from_gcs_to_bigquery >> end