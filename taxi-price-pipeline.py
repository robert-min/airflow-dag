from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

with DAG(dag_id="taxi-price-pipeline",
         schedule_interval="@daily",
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:

    # preprocessing
    preprocess = SparkSubmitOperator(
        application="/Users/robertmin/PycharmProjects/study/data_engineering/airflow_review/preprocessing.py",
        task_id="preprocess",
        conn_id="spark_local"
    )

    # tune hyperparameter
    hyperparameter = SparkSubmitOperator(
        application="/Users/robertmin/PycharmProjects/study/data_engineering/airflow_review/hyperparameter.py",
        task_id="hyperparameter",
        conn_id="spark_local"
    )

    # train model
    train = SparkSubmitOperator(
        application="/Users/robertmin/PycharmProjects/study/data_engineering/airflow_review/train_model.py",
        task_id="train",
        conn_id="spark_local"
    )

    preprocess >> hyperparameter >> train