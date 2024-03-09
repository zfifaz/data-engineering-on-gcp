from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils import timezone


default_args = {
    "start_date": timezone.datetime(2024, 2, 25),
    "owner": "Kan Ouivirach",
}
with DAG(
    "titanic_survivor_predictor_modeling",
    default_args=default_args,
    schedule="@daily",
    tags=["titanic", "bigquery"],
):

    train_model_1 = BigQueryExecuteQueryOperator(
        task_id="train_model_1",
        sql="""
            create or replace model `pim_titanic_12345.survivor_predictor_1`
            options(model_type='logistic_reg') as
            select
                Sex,
                Survived as label
            from `pim_titanic_12345.titanic`
        """,
        gcp_conn_id="my_gcp_conn",
        use_legacy_sql=False,
    )

    train_model_2 = BigQueryExecuteQueryOperator(
        task_id="train_model_2",
        sql="""
            create or replace model `pim_titanic_12345.survivor_predictor_2`
            options(model_type='logistic_reg') as
            select
                Sex,
                Age,
                Survived as label
            from `pim_titanic_12345.titanic`
        """,
        gcp_conn_id="my_gcp_conn",
        use_legacy_sql=False,
    )
