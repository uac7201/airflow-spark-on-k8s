from datetime import timedelta
import os
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
APP_FILE = "spark-load-data.yaml"


with DAG(
    dag_id="encore-workflow-demo",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule=None,                              # keep None while testing conf
    catchup=False,
    render_template_as_native_obj=True,         # preserve int/bool types
    params={                                    # defaults (overridden by dag_run.conf)
        "spark_namespace": "spark-operator",
        "spark_image": "mabi/encore-data-loader:latest",
        "executor_instances": 1,
        "main_file": "local:///opt/app/load_postgres_data.py",
        "APP_NAME": "encore-workflow-demo",

        "USERNAME": "postgres",
        "PASSWORD": "mysecretpassword",
    },
    tags=["spark", "kubernetes", "spark-operator", "encore"],
) as dag:
    

    load_data = SparkKubernetesOperator(
        task_id="load_data",
        application_file=APP_FILE,   # resolves via template_searchpath
        namespace="{{ dag_run.conf.get('spark_namespace', params.spark_namespace) }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        params={
            "APP_NAME": "encore-transform",
            "main_file": "local:///opt/app/transform_data.py",
            "USERNAME": "analytics_user",
            "PASSWORD": "another_password",
            "executor_instances": 2,
        },
    )

    load_data
