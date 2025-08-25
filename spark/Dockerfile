# Dockerfile
FROM docker.io/library/spark:4.0.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates && rm -rf /var/lib/apt/lists/*
ENV PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
WORKDIR /opt/app
COPY requirements.txt /opt/requirements.txt
RUN bash -lc 'if [ -s /opt/requirements.txt ]; then python3 -m pip install --no-cache-dir -r /opt/requirements.txt; fi'
# rename to the path your SparkApplication expects
COPY spark_job.py /opt/app/spark_job.py
# drop privileges
RUN useradd -ms /bin/bash appuser && chown -R appuser:appuser /opt/app
USER appuser
# (do NOT override the CMD/ENTRYPOINT; keep Spark’s default so the operator can start the driver)
