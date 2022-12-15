# Overview

This is an Astro CLI project that demonstrates how to use the astronomer-cosmos package. To get started, clone this repo and run `astro dev start` in this folder to start the project.

![DBT Task Group](/static/dbt_task_group.png)

# dbt

This instance includes a `dbt` folder containing multiple dbt projects, as well as a `dags/dbt` folder that uses the astronomer-cosmos package to run dbt projects as Airflow DAGs.

There are a few changes made to the default Astro CLI project to support this:

1. Add `gcc` and `python3-dev` to the packages.txt file. These aren't included by default to make the base image smaller, but is required for `astronomer-cosmos`'s underlying packages.
2. Add `astronomer-cosmos` to the requirements.txt file. This is the package that allows you to run dbt projects as Airflow DAGs.
3. Add the following to the Dockerfile to run dbt in a virtual environment:

```Dockerfile
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
```

The `dags/` folder contains a few DAGs that demonstrate how to run dbt projects as Airflow DAGs. They're configured to point to the Airflow database (defined in `airflow_settings.yaml`) so they'll work out of the box!
