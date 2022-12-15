FROM quay.io/astronomer/astro-runtime:7.0.0
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true

# #Installs locally
# USER root
# COPY /astronomer-cosmos/ /astronomer-cosmos
# WORKDIR "/usr/local/airflow/astronomer-cosmos"
# # RUN pip install -e . --no-dependencies # useful for testing pyenv
# RUN pip install -e .
USER astro

#Install then venv /usr/local/airflow/dbt_venv/bin/activate
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate