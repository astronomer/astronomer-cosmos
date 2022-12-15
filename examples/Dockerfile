FROM quay.io/astronomer/astro-runtime:7.0.0

USER root

# mount the local dbt directory to the container, rw for dbt to write logs
ADD dbt /usr/local/airflow/dbt
# make sure the dbt directory is owned by the astro user
RUN chown -R astro:astro /usr/local/airflow/dbt

USER astro

# install the venv for dbt
# create and activate the virtual environment
RUN python -m virtualenv dbt_venv && \
    source dbt_venv/bin/activate && \
    # update dbt.postgres to the database you are using
    pip install astronomer-cosmos[dbt.postgres] && \
    deactivate
