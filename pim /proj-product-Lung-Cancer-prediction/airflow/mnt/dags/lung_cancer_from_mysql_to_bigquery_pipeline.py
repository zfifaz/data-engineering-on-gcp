import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account






def _load_to_gcs():
    BUSINESS_DOMAIN = "lung_cancer"
    location = "us-central1"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs = "/opt/airflow/dags/pim-titanic-load-to-gcs-8e2f0af6fe47.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    project_id = Variable.get("pim-titanic-load-to-gcs-secret")

    # Load data from Local to GCS
    bucket_name = Variable.get("lung_cancer_bucket_name")
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = "/opt/airflow/dags/Lung_Cancer_Dataset.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/Lung_Cancer_Dataset.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_from_gcs_to_bigquery():
    BUSINESS_DOMAIN = "lung_cancer"
    location = "us-central1"

    bucket_name = Variable.get("lung_cancer_bucket_name")
    destination_blob_name = f"{BUSINESS_DOMAIN}/Lung_Cancer_Dataset.csv"

    keyfile_bigquery = "/opt/airflow/dags/pim-titanic-load-gcs-to-bigquery-b6e08f465048.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    project_id = Variable.get("Project_ID_key")

    # # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )
    table_id = f"{project_id}.lung_cancer_09032024.lung_cancer"
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


default_args = {
    "start_date": timezone.datetime(2024, 2, 25),
    "owner": "Kan Ouivirach",
}
with DAG(
    "lung_cancer_from_mysql_to_bigquery_pipeline",
    default_args=default_args,
    schedule=None,
    tags=["lung_cancer", "mysql", "bigquery"],
):

  

    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=_load_to_gcs,
    )

    load_from_gcs_to_bigquery = PythonOperator(
        task_id="load_from_gcs_to_bigquery",
        python_callable=_load_from_gcs_to_bigquery,
    )

    load_to_gcs >> load_from_gcs_to_bigquery

# MySQL instance
# cPi929fLijts