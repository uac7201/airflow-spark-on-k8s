# airflow/dags/hello_world.py
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils import timezone

default_args = {
    "owner": "Howdy",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="hello_world",
    start_date=timezone.datetime(2025, 8, 24, tzinfo=timezone.utc),  # static, tz-aware
    schedule=None,            # run manually; set to "@daily" if you want it scheduled
    catchup=False,
    default_args=default_args,
    tags=["example", "quickstart"],
)
def hello_world():
    @task()
    def say_hello():
        import platform, os
        return {
            "message": "Hello from Airflow!",
            "python": platform.python_version(),
            "hostname": getattr(os, "uname", lambda: type("x", (), {"nodename": "unknown"})())().nodename
                       if hasattr(os, "uname") else os.getenv("HOSTNAME", "unknown"),
            "now_utc": timezone.utcnow().isoformat(),
        }

    @task()
    def print_result(info: dict):
        print(f"{info['message']}")
        print(f"Python: {info['python']} | Host: {info['hostname']}")
        print(f"UTC now: {info['now_utc']}")

    print_result(say_hello())

dag = hello_world()
