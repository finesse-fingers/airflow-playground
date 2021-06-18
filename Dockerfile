FROM apache/airflow:2.1.0

RUN set -ex \
    && pip install scipy \
    && pip install acryl-datahub \
    && pip install great_expectations \
    && pip install 'dbt<0.19' \
    && pip uninstall -y SQLAlchemy \
    && pip install SQLAlchemy==1.3.15 \
    && pip install confluent_kafka \
    && pip install fastavro

# having this is useful if we want to configure lineage
COPY deploy/config/airflow.cfg /opt/airflow/airflow.cfg
