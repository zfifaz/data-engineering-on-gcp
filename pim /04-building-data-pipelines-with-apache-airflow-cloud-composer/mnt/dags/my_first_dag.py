from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

with DAG (
    dag_id="my_first_dag",
    start_date=timezone.datetime(2024, 1, 22),
    schedule="0 0 * * *",

):  
    my_first_task = EmptyOperator(task_id="my_first_task")
    my_second_task = EmptyOperator(task_id="my_second_task")

    my_first_task >> my_second_task