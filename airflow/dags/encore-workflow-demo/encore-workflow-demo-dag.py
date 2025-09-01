from datetime import timedelta
import os
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.models import Variable

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="encore-workflow-demo",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    template_searchpath=[_THIS_DIR],  # so the YAMLs resolve
    params={  # DAG-level defaults (used if not overridden by task params or dag_run.conf)
        "spark_namespace": "spark-operator",
        "APP_NAME": "encore-workflow-demo",
        "PATH": "/shared/encore/tmp/widgets",
        "executor_instances": 1,
    },
    tags=["spark", "kubernetes", "spark-operator", "encore"],
) as dag:

    # ---------- Task 1: load_data (your current reader) ----------
    load_data = SparkKubernetesOperator(
        task_id="load_data",
        application_file="spark-load-data.yaml",
        namespace="{{ dag_run.conf.get('spark_namespace', params.spark_namespace) }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        params={
            "spark_image": "mabi/encore-spark-data-loader:latest",
            "spark_version": "4.0.0",
            "main_file": "local:///opt/app/load_postgres_data.py",
            
            "USERNAME": "postgres",
            "PASSWORD": "mysecretpassword",
        },
    )

    # ---------- Task 2: write_polaris (Polaris-only writer) ----------
    write_polaris = SparkKubernetesOperator(
        task_id="write_polaris",
        application_file="spark-polaris-writer.yaml",
        namespace="{{ dag_run.conf.get('spark_namespace', params.spark_namespace) }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        params={
            # use a different image / Spark version / entrypoint for Iceberg REST
            "spark_image_iceberg": "mabi/encore-spark-polaris-writer:latest",
            "spark_version": "3.5.1",
            "main_file": "local:///opt/app/polaris_writer.py",
            # Polaris env (read sensitive values from Airflow Variables)
            "POLARIS_URI": "https://enercity-encorepolaris.snowflakecomputing.com/polaris/api/catalog",
            "POLARIS_OAUTH2_SCOPE": "PRINCIPAL_ROLE:snowflake",
            "POLARIS_ALIAS": "polaris", 
            "POLARIS_WAREHOUSE": "azure_lucas", 
            #"POLARIS_OAUTH2_TOKEN_URL": "https://enercity-encorepolaris.snowflakecomputing.com/oauth/token-request",
            "POLARIS_OAUTH2_CLIENT_ID": Variable.get("POLARIS_OAUTH2_CLIENT_ID"),
            "POLARIS_OAUTH2_CLIENT_SECRET": Variable.get("POLARIS_OAUTH2_CLIENT_SECRET"),
            "TARGET_NAMESPACE": "spark_maik",
            "TARGET_TABLE": "maikspark_demo",
            "WRITE_MODE": "append" # must match OUTPUT_PATH in j
        },
    )

    load_data >> write_polaris
