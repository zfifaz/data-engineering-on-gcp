from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

import pandas as pd


def _extract_from_mysql():
    hook = MySqlHook(mysql_conn_id="pim_titanic_mysql_conn")
    # hook.bulk_dump("titanic", "/opt/airflow/dags/titanic_dump.tsv")
    conn = hook.get_conn()

    df = pd.read_sql("select * from titanic", con=conn)
    print(df.head())
    df.to_csv("/opt/airflow/dags/titanic_dump.csv", index=False)


def _load_to_gcs():
    BUSINESS_DOMAIN = "titanic"
    location = "us-central1"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs = "/opt/airflow/dags/pim-titanic-load-to-gcs-409709-93027940e878.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    project_id = "data-engineering-on-gcp-409709"

    # Load data from Local to GCS
    bucket_name = "pim-27361"
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = "/opt/airflow/dags/titanic_dump.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/titanic.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


default_args = {
    "start_date": timezone.datetime(2024, 2, 25),
    "owner": "Kan Ouivirach",
}
with DAG(
    "titanic_from_mysql_to_bigquery_pipeline",
    default_args=default_args,
    schedule=None,
    tags=["titanic", "mysql", "bigquery"],
):

    extract_from_mysql = PythonOperator(
        task_id="extract_from_mysql",
        python_callable=_extract_from_mysql,
    )

    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=_load_to_gcs,
    )

    load_from_gcs_to_bigquery = EmptyOperator(task_id="load_from_gcs_to_bigquery")

    extract_from_mysql >> load_to_gcs >> load_from_gcs_to_bigquery
