FROM quay.io/jupyter/pyspark-notebook:latest

USER root

COPY requirements-spark.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf /tmp/requirements.txt

USER jovyan

WORKDIR /home/jovyan