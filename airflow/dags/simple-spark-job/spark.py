# airflow/dags/spark-example/spark.py
from datetime import timedelta
import os

from airflow import DAG
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = _THIS_DIR
APP_FILE = "spark-app.yaml"

default_args = {
    "owner": "Howdy",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_spark_job",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    template_searchpath=[TEMPLATE_DIR],
    params={
        "spark_namespace": "spark-operator",
        "spark_image": "mabi/simple-spark-job:latest",
        "executor_instances": 1,
        "app_suffix": "",
        "main_file": "local:///opt/app/spark_job.py",
        "spark_app_name": "simple-spark-job",  # used by YAML metadata.name
    },
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,                 # Jinja-rendered using params
        namespace="{{ params.spark_namespace }}",  # where the CR will be created
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )
