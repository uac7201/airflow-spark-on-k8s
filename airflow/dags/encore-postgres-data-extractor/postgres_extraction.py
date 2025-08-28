from datetime import timedelta
import os
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


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
    schedule=None,                              # keep None while testing conf
    catchup=False,
    render_template_as_native_obj=True,         # preserve int/bool types
    params={                                    # defaults (overridden by dag_run.conf)
        "spark_namespace": "spark-operator",
        "spark_image": "mabi/postgres_extractor_job:latest",
        "executor_instances": 1,
        "main_file": "local:///opt/app/postgres_to_polaris.py",
        "APP_NAME": "postgres-extractor-job",
        "JDBC_URL": "jdbc:postgresql://host:5432/dbname",
        "JDBC_USERNAME": "your_username",
        "JDBC_PASSWORD": "your_password",
        "JDBC_DRIVER": "org.postgresql.Driver",
        "JDBC_TABLE": "public.orders",
        "JDBC_QUERY": "SELECT * FROM public.orders",
    },
    tags=["spark", "kubernetes", "spark-operator"],
) as dag:
    
    print_conf = BashOperator(
        task_id="print_conf",
        # Pretty-print if jq is available; otherwise just echo JSON
        bash_command="echo '{{ dag_run.conf | tojson }}' | jq . || echo '{{ dag_run.conf | tojson }}'"
    )

    submit_spark = SparkKubernetesOperator(
        task_id="submit_spark",
        application_file=APP_FILE,   # resolves via template_searchpath
        namespace="{{ dag_run.conf.get('spark_namespace', params.spark_namespace) }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    print_conf >> submit_spark
