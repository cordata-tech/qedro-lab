# Airflow with its OpenLineage provider, and dbt and art30-emit in a virtualenv of
# their own: dbt and Airflow pin conflicting dependencies, and keeping them apart is
# how most deployments run dbt from Airflow. pipeline-runtime gets a third, for the
# same reason.
FROM apache/airflow:3.4.0-python3.12

COPY requirements-airflow.txt requirements-tools.txt requirements-pipeline-runtime.txt /tmp/
RUN pip install --no-cache-dir "apache-airflow==3.4.0" -r /tmp/requirements-airflow.txt

USER root
RUN python -m venv /opt/tools && python -m venv /opt/pipeline-runtime \
 && chown -R airflow: /opt/tools /opt/pipeline-runtime \
 && mkdir -p /warehouse && chown airflow:0 /warehouse && chmod 2775 /warehouse
USER airflow
RUN /opt/tools/bin/pip install --no-cache-dir -r /tmp/requirements-tools.txt
RUN /opt/pipeline-runtime/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline-runtime.txt
