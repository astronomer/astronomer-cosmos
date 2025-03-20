# Run Airflow3 Locally

This guide walks you through setting up Apache Airflow 3 locally using Hatch and a Postgres container. You'll run a Postgres container to use it as a backend database for Airflow and set up the necessary environment to run Airflow.

## 1. Setup Postgres Container (Optional)

We'll use PostgreSQL as the backend. The following command will pull the official Postgres image, create a container named postgres, and expose the necessary ports.

```commandline
docker run --name postgres -p 5432:5432 -p 5433:5433 -e POSTGRES_PASSWORD=postgres postgres
```

## 2. Access the PostgreSQL Console and Create the Database

Now that the PostgreSQL container is running, you can connect to it via the command line using psql

```commandline
psql --u postgres
```

### Create the Database for Airflow

Once you're inside the psql interactive terminal, you can create a new database that Airflow will use.

```commandline
CREATE DATABASE airflow_db;
```

## 3. Setup Virtual Environment for Airflow3

With your Postgres container running and your database set up, you need to configure the virtual environment for Airflow3.

### Export ENV

This will export the AIRFLOW related env like AIRFLOW_HOME etc

```commandline
source scripts/airflow3/env.sh
```

### Install Dependency

```commandline
sh scripts/airflow3/setup.sh
```

## 5. Run Airflow in Standalone Mode

Activate the virtual env created in previous step and run airflow

```commandline
source "$(pwd)/scripts/airflow3/venv/bin/activate"

airflow standalone
```

This command will:

- Set the necessary environment variables (like AIRFLOW_HOME).
- Initialize the Airflow database.
- Start Airflow webserver, scheduler and trigger.

### Run Airflow Tests

Once Airflow is running, you can also run tests.

```commandline
source scripts/airflow3/env.sh

source "$(pwd)/scripts/airflow3/venv/bin/activate"

sh scripts/airflow3/tests.sh
```

## 4. Access the Airflow Web Interface

After running the standalone command, you can access the Airflow web interface to monitor the status of your DAGs, tasks, and more.

- The web interface should be available at http://localhost:8080
