Overview
========

Welcome to Astronomer! This project was shows a few examples of using Astronomer Cosmos. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine, and finally how to write and execute basic dbt projects in Airflow. 

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes example dags that map one-to-one to the dbt projects shown in /dbt.
- dbt: This folder contains the dbt projects we're running and rendering in our dags! [These specific projects are all sourced from dbt labs.](https://docs.getdbt.com/faqs/project/example-projects) Go check them out!
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here. This file also creates a virtual environment for your dbt executions.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It only requires astronomer-cosmos by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project. By default, it includes a connection to your Airflow Metadata DB for quick and easy execution of our example DAGs.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 3 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database (and dbt target database when running example DAGs)
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks

2. Verify that all 3 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://docs.astronomer.io/cloud/deploy-code/

Contact
=======

Astronomer Cosmos is in early stages. To report a bug or suggest a change, reach out to our support team: https://support.astronomer.io/
