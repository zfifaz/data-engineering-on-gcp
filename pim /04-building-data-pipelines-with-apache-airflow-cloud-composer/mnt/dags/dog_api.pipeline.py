import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import requests


DAGS_FOLDER = "/opt/airflow/dags"


def _get_dog_image_url():
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()
    
    with open(f"{DAGS_FOLDER}/dogs.json", "w") as f:
        json.dump(data, f)


with DAG(
    dag_id="dog_api_pipeline",
    start_date=timezone.datetime(2024, 1, 28),
    schedule="*/30 * * * *",
    catchup=False,
):
    start = EmptyOperator(task_id="start")

    get_dog_image_url = PythonOperator(
        task_id="get_dog_image_url",
        python_callable=_get_dog_image_url,
    )

    load_to_jsonbin = BashOperator(
        task_id="load_to_jsonbin",
        bash_command=f"""
            API_KEY='{{{{ var.value.jsonbin_api_key }}}}'
            COLLECTION_ID='{{{{ var.value.jsonbin_dog_collection_id }}}}'

            curl -XPOST \
                -H "Content-type: application/json" \
                -H "X-Master-Key: $API_KEY" \
                -H "X-Collection-Id: $COLLECTION_ID" \
                -d @{DAGS_FOLDER}/dogs.json \
                "https://api.jsonbin.io/v3/b"
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> get_dog_image_url >> load_to_jsonbin >> end