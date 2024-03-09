from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils import timezone


default_args = {
    "start_date": timezone.datetime(2024, 2, 25),
    "owner": "Kan Ouivirach",
}
with DAG(
    "lung_cancer_predictor_modeling",
    default_args=default_args,
    tags=["lung_cancer", "bigquery"],
):

    train_model_1 = BigQueryExecuteQueryOperator(
        task_id="train_model_1",
        sql="""
            create or replace model `lung_cancer_09032024.cancer_predictor_1`
            options(model_type='logistic_reg') as
            select
                GENDER,
                AGE,
                SMOKING,
                YELLOW_FINGERS,
                ANXIETY,
                PEER_PRESSURE,
                CHRONIC_DISEASE,
                FATIGUE, 	
                ALLERGY, 	
                WHEEZING,	
                ALCOHOL_CONSUMING,	
                COUGHING,	
                SHORTNESS_OF_BREATH,
                SWALLOWING_DIFFICULTY,	
                CHEST_PAIN,	
                LUNG_CANCER as label
            from `lung_cancer_09032024.lung_cancer`
        """,
        gcp_conn_id="my_gcp_conn",
        use_legacy_sql=False,
    )

