from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_time': datetime(2021, 1, 1)
}

with DAG(dag_id="taxi-price-pipeline",
         schedule_interval="@daily",
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:

    # preprocessing
    pass

    # tune hyperparameter

    # train model