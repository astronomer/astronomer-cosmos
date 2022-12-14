# Embed This Project in Astro - Hack Week Sandbox!!!

1. run ``git clone git@github.com:astronomer/airflow-dbt-blog.git && cd airflow-dbt-blog``
2. run ``git checkout hack-week``
3. run ``git clone git@github.com:astronomer/cosmos.git``
4. change the ``docker-compose.override.yml`` (in the ``airflow-dbt-blog`` directory):

  ```yaml
    version: "3.1"
    services:
      scheduler:
        volumes:
          - ./dbt:/usr/local/airflow/dbt:rw
          - ./cosmos:/usr/local/airflow/cosmos:rw

      webserver:
        volumes:
          - ./dbt:/usr/local/airflow/dbt:rw
          - ./cosmos:/usr/local/airflow/cosmos:rw

      triggerer:
        volumes:
          - ./dbt:/usr/local/airflow/dbt:rw
          - ./cosmos:/usr/local/airflow/cosmos:rw
  ```

4. change the ``Dockerfile`` (in the ``airflow-dbt-blog`` directory) to be this:

  ```dockerfile
  FROM quay.io/astronomer/astro-runtime:7.0.0
  ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true

  #Installs locally
  USER root
  COPY /cosmos/ /cosmos
  WORKDIR "/usr/local/airflow/cosmos"
  RUN pip install -e .

  WORKDIR "/usr/local/airflow"

  USER astro
  ```

5. After you've made those changes, then run an ``astro dev start`` command (from the ``airflow-dbt-blog`` directory) to
spin up a sandbox that mounts this cosmos repo for quick e2e testing!

6. Happy hacking!
