FROM quay.io/astronomer/astro-runtime:10.4.0-base
# https://docs.astronomer.io/astro/upgrade-runtime
#   astro dev upgrade-test --runtime-version 10.4.0
#     results in: tests\upgrade-test-10.0.0--10.4.0\dag-test-report.html
#       4 fails predictable, not sqlite etc, but 8 passes included various cosmos tests >> upgrade, modify Dockerfile and astro dev restart
#
#         before in UI Astronomer Runtime 10.0.0 based on Airflow 2.8.0+astro.1
#         after  in UI Astronomer Runtime 10.4.0 based on Airflow 2.8.2+astro.1
#         trigger basic_cosmos_dag SUCCESS
USER root

COPY ./pyproject.toml  ${AIRFLOW_HOME}/astronomer_cosmos/
COPY ./README.rst  ${AIRFLOW_HOME}/astronomer_cosmos/
COPY ./cosmos/  ${AIRFLOW_HOME}/astronomer_cosmos/cosmos/

# install the package in editable mode
RUN pip install -e "${AIRFLOW_HOME}/astronomer_cosmos"[dbt-postgres,dbt-snowflake]

# make sure astro user owns the package
RUN chown -R astro:astro ${AIRFLOW_HOME}/astronomer_cosmos

USER astro

# add a connection to the airflow db for testing
ENV AIRFLOW_CONN_AIRFLOW_DB=postgres://airflow:pg_password@postgres:5432/airflow
ENV DBT_ROOT_PATH=/usr/local/airflow/dags/dbt
ENV DBT_DOCS_PATH=/usr/local/airflow/dbt-docs
