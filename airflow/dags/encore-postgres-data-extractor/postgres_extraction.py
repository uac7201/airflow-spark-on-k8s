# airflow/dags/spark-example/spark.py
from datetime import timedelta
import os

from airflow import DAG
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = _THIS_DIR
APP_FILE = "spark-postgres-extractor.yaml"

default_args = {
    "owner": "Howdy",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_data_from_postgres_job",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    template_searchpath=[TEMPLATE_DIR],
    params={
        "spark_namespace": "spark-operator",
        "spark_image": "mabi/postgres_extractor_job:latest",
        "executor_instances": 1,
        "main_file": "local:///opt/app/postgres_to_polaris.py",
        "spark_app_name": "postgres-extractor-job",
        # Added environment variables
        "JDBC_URL": "jdbc:postgresql://host:5432/dbname",
        "JDBC_USERNAME": "your_username",
        "JDBC_PASSWORD": "your_password",
        "JDBC_DRIVER": "org.postgresql.Driver",
        "JDBC_TABLE": "public.orders",
        "JDBC_QUERY": "SELECT * FROM public.orders",
        "APP_NAME": "postgres-extractor-job",
    },
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,  # Jinja-rendered using params
        namespace="{{ params.spark_namespace }}",  # where the CR will be created
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    # airflow/dags/spark-example/spark.py
from datetime import timedelta
import os

from airflow import DAG
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.operators.bash import BashOperator  # <-- added

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = _THIS_DIR
APP_FILE = "spark-postgres-extractor.yaml"

default_args = {
    "owner": "Howdy",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="extract_data_from_postgres_job",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    template_searchpath=[TEMPLATE_DIR],
    params={
        "spark_namespace": "spark-operator",
        "spark_image": "mabi/postgres_extractor_job:latest",
        "executor_instances": 1,
        "main_file": "local:///opt/app/postgres_to_polaris.py",
        "spark_app_name": "postgres-extractor-job",
        # Added environment variables
        "JDBC_URL": "jdbc:postgresql://host:5432/dbname",
        "JDBC_USERNAME": "your_username",
        "JDBC_PASSWORD": "your_password",
        "JDBC_DRIVER": "org.postgresql.Driver",
        "JDBC_TABLE": "public.orders",
        "JDBC_QUERY": "SELECT * FROM public.orders",
        "APP_NAME": "postgres-extractor-job",
    },
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,  # Jinja-rendered using params
        namespace="{{ params.spark_namespace }}",  # where the CR will be created
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    sleep_1min = BashOperator(task_id="sleep_1min", bash_command="sleep 60")

    submit_spark >> sleep_1min
