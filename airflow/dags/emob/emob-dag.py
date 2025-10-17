from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
import os
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

NAMESPACE = "airflow"
SERVICE_ACCOUNT = "spark"
PVC_NAME = "airflow-dynamic-pvc-emob"
KUBECTL_IMG = "mabi/parquet-creator:7786a7977ab6c3e4436c4496f8a9cc47eba343f4"
MOUNT_PATH = "/shared/encore"

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = _THIS_DIR

SECCTX = k8s.V1SecurityContext(
    run_as_non_root=True,
    run_as_user=1000,
    run_as_group=1000,
    allow_privilege_escalation=False,
    read_only_root_filesystem=False,
    capabilities=k8s.V1Capabilities(drop=["ALL"]),
)
RES = k8s.V1ResourceRequirements(
    requests={"cpu": "50m", "memory": "64Mi"},
    limits={"cpu": "200m", "memory": "256Mi"},
)

WORK_VOL = k8s.V1Volume(
    name="work",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name=PVC_NAME
    ),
)
WORK_MOUNT = k8s.V1VolumeMount(name="work", mount_path=MOUNT_PATH)

with DAG(
    dag_id="emob",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=[TEMPLATE_DIR],
    default_args={"owner": "airflow", "retries": 0},
) as dag:

    setup_pvc = KubernetesPodOperator(
        task_id="setup_pvc",
        name="setup-pvc",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=KUBECTL_IMG,
        image_pull_policy="IfNotPresent",
        cmds=["python3", "/app/app.py"],
        arguments=[
            "apply",
            "--name",
            PVC_NAME,
            "--namespace",
            NAMESPACE,
            "--storage",
            "15Gi",
            "--storage-class-name",
            "azurefile-csi",
            "--access-mode",
            "ReadWriteMany",
            "--volume-mode",
            "Filesystem",
            "--label",
            "owner=airflow",
            "--label",
            "purpose=handoff",
        ],
        container_security_context=SECCTX,
        container_resources=RES,
        get_logs=True,
        is_delete_operator_pod=True,
        retries=3,
        retry_delay=timedelta(seconds=10),
    )

    load_data = KubernetesPodOperator(
        task_id="create_parquet_file",
        name="reader",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image="mabi/parquet-creator:7786a7977ab6c3e4436c4496f8a9cc47eba343f4",
        image_pull_policy="IfNotPresent",
        cmds=["python", "generate_parquet.py"],
        arguments=[
            "--num-customers",
            "1000",
            "--output-path",
            "/shared/encore/namespace/project/customer/customer_data.parquet",
        ],
        volumes=[WORK_VOL],
        volume_mounts=[WORK_MOUNT],
        container_security_context=SECCTX,
        container_resources=RES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    """ load_data = SparkKubernetesOperator(
        task_id="load_data",
        application_file="spark-apps/spark-data-load.yaml",
        kubernetes_conn_id="kubernetes_default",
        namespace=NAMESPACE,
        do_xcom_push=False,
        params={
            "PVC_CLAIM_NAME": PVC_NAME,
            "EMOB_DB_PASSWORD": Variable.get("EMOB_DB_PASSWORD"),
        },
    ) 
    """

    file_check = KubernetesPodOperator(
        task_id="list_parquet_files",
        name="reader",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image="busybox:1.36.1",
        image_pull_policy="IfNotPresent",
        cmds=["/bin/sh", "-c"],
        arguments=[
            'echo "Listing all parquet files under /shared/encore..." && '
            'find /shared/encore -type f -name "*.parquet" -print || '
            'echo "No parquet files found."'
        ],
        volumes=[WORK_VOL],
        volume_mounts=[WORK_MOUNT],
        container_security_context=SECCTX,
        container_resources=RES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    write_to_polaris = SparkKubernetesOperator(
        task_id="write_to_polaris",
        application_file="spark-apps/spark-data-write.yaml",
        kubernetes_conn_id="kubernetes_default",
        namespace=NAMESPACE,
        do_xcom_push=False,
        params={
            "PVC_CLAIM_NAME": PVC_NAME,
            "INPUT_PATH": MOUNT_PATH,
            "POLARIS_URI": "https://enercity-encorepolaris.snowflakecomputing.com/polaris/api/catalog",
            "POLARIS_OAUTH2_SCOPE": "PRINCIPAL_ROLE:ALL",
            "POLARIS_ALIAS": "encoredev",
            "POLARIS_WAREHOUSE": "encoredev",
            "POLARIS_OAUTH2_CLIENT_ID": Variable.get("POLARIS_OAUTH2_CLIENT_ID"),
            "POLARIS_OAUTH2_CLIENT_SECRET": Variable.get(
                "POLARIS_OAUTH2_CLIENT_SECRET"
            ),
        },
    )

    cleanup_pvc = KubernetesPodOperator(
        task_id="cleanup_pvc",
        name="cleanup-pvc",
        namespace=NAMESPACE,
        service_account_name=SERVICE_ACCOUNT,
        image=KUBECTL_IMG,
        image_pull_policy="IfNotPresent",
        cmds=["python3", "/app/app.py"],
        arguments=[
            "delete",
            "--name",
            PVC_NAME,
            "--namespace",
            NAMESPACE,
        ],
        trigger_rule=TriggerRule.ALL_DONE,  # Always run cleanup
        container_security_context=SECCTX,
        container_resources=RES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # DAG fails if any upstream fails
    )

    # Task dependencies
    setup_pvc >> load_data
    load_data >> file_check >> write_to_polaris
    write_to_polaris >> cleanup_pvc
    [load_data, write_to_polaris, cleanup_pvc] >> end
