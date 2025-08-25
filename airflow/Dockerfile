FROM apache/airflow:3.0.2

COPY requirements.txt /opt/requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /opt/requirements.txt