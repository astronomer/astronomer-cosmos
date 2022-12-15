FROM quay.io/astronomer/astro-runtime:7.0.0

# install the venv for dbt
# create and activate the virtual environment
RUN python -m virtualenv dbt_venv && \
    source dbt_venv/bin/activate && \
    # update dbt.postgres to the database you are using
    pip install astronomer-cosmos[dbt.postgres] && \
    deactivate
