# Apache Airflow — Incremental ETL Pipeline

An end-to-end Data Engineering project focused on ETL orchestration, incremental data processing, data quality and historical data management using Apache Airflow, Python, Pandas, MySQL and Docker.

The project simulates a customer data integration scenario in which changes from an operational database are captured, processed through an ETL pipeline and loaded into a Data Warehouse.

The solution implements incremental extraction, execution monitoring and Slowly Changing Dimension Type 2 (SCD Type 2) to preserve historical customer information.

Project Overview

The main objectives of this project are:

Orchestrate ETL workflows using Apache Airflow.

Extract customer data from a MySQL OLTP database.

Implement incremental data extraction based on change tracking.

Transform and standardize data using Python and Pandas.

Separate operational, staging and analytical data layers.

Maintain historical customer records using SCD Type 2.

Monitor ETL executions and manage incremental load checkpoints.

Containerize the environment using Docker Compose.

Architecture

The solution uses a layered data architecture, separating operational data, staging and analytical storage.

flowchart TD
    subgraph SOURCE["Source Layer - MySQL"]
        OLTP["ERP_OLTP"]
        CLIENT["CLIENTE"]
        LOG["LOG_CLIENTE"]
        OLTP --> CLIENT
        CLIENT --> LOG
    end

    subgraph AIRFLOW["Orchestration - Apache Airflow"]
        DAG["etl_client DAG"]
        EXTRACT["Extract"]
        TRANSFORM["Transform - Python / Pandas"]
        LOAD["Load - SCD Type 2"]

        DAG --> EXTRACT
        EXTRACT --> TRANSFORM
        TRANSFORM --> LOAD
    end

    subgraph TARGET["Data Warehouse - MySQL"]
        STAGE["ERP_STAGE"]
        DW["ERP_DW"]
        DIM["DWCD_CLIENTE"]
        CONTROL["ETL_EXECUCAO / ETL_CONTROLE"]

        STAGE --> DW
        DW --> DIM
    end

    META["PostgreSQL - Airflow Metadata"]

    LOG --> EXTRACT
    EXTRACT --> STAGE
    LOAD --> DIM
    DAG -.-> META
    DAG -.-> CONTROL

Database Architecture

Database

Purpose

ERP_OLTP

Operational database containing customer records and change logs.

ERP_STAGE

Staging database used during ETL processing.

ERP_DW

Data Warehouse containing analytical structures and ETL control tables.

PostgreSQL

Stores Apache Airflow metadata and workflow execution information.

ETL Workflow

The etl_client DAG is responsible for orchestrating the customer data pipeline.

The process is divided into the following stages:

Step

Process

Description

1

Start Execution

Registers the beginning of an ETL execution.

2

Prepare Staging

Clears the staging table before processing a new batch.

3

Extract

Retrieves customer changes from the OLTP change log.

4

Transform

Standardizes and prepares the extracted data using Python and Pandas.

5

Load

Loads the processed data into the Data Warehouse using SCD Type 2.

6

Finish Execution

Records the final execution status and updates the incremental control timestamp after successful processing.

Incremental Data Loading

The pipeline implements an incremental extraction strategy to avoid processing the entire source database during every execution.

Two control tables are used:

ETL_EXECUCAO

Stores information about each ETL execution:

Execution ID.

Process name.

Start timestamp.

End timestamp.

Execution status.

Error information.

ETL_CONTROLE

Stores the last successful processing timestamp for each ETL process.

The field DT_ULTIMA_CARGA acts as a watermark for incremental extraction.

The extraction process uses this timestamp to identify new or modified records in LOG_CLIENTE.

The control timestamp is updated only after a successful execution, preventing failed executions from advancing the incremental processing reference.

Historical Data Management — SCD Type 2

The Data Warehouse uses Slowly Changing Dimension Type 2 to maintain customer history.

Instead of overwriting existing records, the process preserves previous versions whenever a tracked attribute changes.

The dimension uses the following concepts:

Field

Description

IDSK

Surrogate key identifying each dimension version.

IDCLIENTE

Business key from the operational database.

INICIO

Timestamp when the dimension version becomes valid.

FIM

Timestamp when the dimension version expires.

When a tracked attribute changes:

The current dimension record is closed by updating its FIM timestamp.

A new dimension record is inserted with the updated information.

Previous versions remain available for historical analysis.

A record with FIM IS NULL represents the current version of the customer.

This approach allows historical analysis of customer information at different points in time.

Technology Stack

Technology

Application

Apache Airflow 3

Workflow orchestration

Python 3.12

ETL development

Pandas

Data transformation and manipulation

SQLAlchemy

Database connectivity and ORM

PyMySQL

MySQL connectivity

MySQL 8.4

OLTP, Staging and Data Warehouse

PostgreSQL 16

Airflow metadata database

Docker

Containerization

Docker Compose

Environment orchestration

Repository Structure

.
├── airflow/
│   └── dags/
│       ├── config/
│       │   └── database.py
│       ├── models/
│       │   └── database_models.py
│       ├── services/
│       │   └── cliente.py
│       ├── CLIENTES_ESTADOS.xlsx
│       └── etl_client.py
│
├── mysql-init/
│
├── Dockerfile
├── docker-compose.yml
├── setup.sql
└── README.md

Main Components

File / Directory

Description

etl_client.py

Defines the Airflow DAG and task dependencies.

services/cliente.py

Implements customer extraction, transformation, loading and execution tracking.

config/database.py

Configures database connections.

models/database_models.py

Defines database models.

CLIENTES_ESTADOS.xlsx

Geographic reference data used during enrichment.

setup.sql

Creates database structures and initial data.

mysql-init/

Contains MySQL initialization support files.

Dockerfile

Defines the custom Airflow image.

docker-compose.yml

Defines the containerized environment.

Getting Started

Prerequisites

Before running the project, make sure you have installed:

Git

Docker Desktop or Docker Engine

Docker Compose

1. Clone the Repository

git clone https://github.com/dlinoba/Airflow.git

cd Airflow

2. Configure Environment Variables

Create a .env file in the project root directory.

Configure the environment variables required by docker-compose.yml.

Example:

# MySQL
MYSQL_ROOT_PASSWORD=change_me_root
MYSQL_DATABASE=ERP_OLTP
MYSQL_USER=etl_user
MYSQL_PASSWORD=change_me_mysql

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=change_me_postgres
POSTGRES_DB=airflow

# Airflow
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=change_me_airflow

Replace the example credentials with your own values.

Important: Never commit real passwords, credentials or sensitive environment variables to GitHub.

3. Build and Start the Environment

Run the following command:

docker compose up -d --build

Check the running containers:

docker compose ps

4. Access Apache Airflow

Open the following URL in your browser:

http://localhost:8080

Log in using the credentials configured in your environment.

Locate the etl_client DAG and trigger a manual execution to test the pipeline.

5. Useful Docker Commands

# Check running containers
docker compose ps

# View service logs
docker compose logs -f

# View Airflow scheduler logs
docker compose logs -f airflow-scheduler

# Stop containers without removing persistent volumes
docker compose down

# Rebuild and start containers
docker compose up -d --build

Database initialization note: MySQL initialization scripts are normally executed only when the database data directory is initialized for the first time. Changes to initialization scripts may not be applied automatically to an existing database volume.

Testing the ETL Pipeline

The project includes sample customer data in the OLTP database.

You can simulate customer changes by executing SQL statements such as:

-- Insert a new customer
INSERT INTO CLIENTE
    (NOME, SOBRENOME, EMAIL, SEXO, NASCIMENTO)
VALUES
    ('Teste', 'Teste', 'teste@teste.com.br', 'M', '2000-03-10');

-- Update an existing customer
UPDATE CLIENTE
SET NOME = 'Kaio'
WHERE IDCLIENTE = 10;

After modifying the source data:

Access the Airflow interface.

Locate the etl_client DAG.

Trigger a new execution.

Monitor the task execution.

Verify the results in the Data Warehouse.

Example validation query:

SELECT *
FROM ERP_DW.ETL_EXECUCAO
ORDER BY ID_EXECUCAO DESC;

SELECT *
FROM ERP_DW.ETL_CONTROLE;

SELECT *
FROM ERP_DW.DWCD_CLIENTE
WHERE IDCLIENTE = 10
ORDER BY INICIO;

For customers with historical changes, previous dimension versions should have their FIM timestamps populated, while the current version should have FIM IS NULL.

Project Scope

This project was developed as a practical Data Engineering portfolio project to demonstrate:

ETL pipeline development.

Workflow orchestration.

Incremental data processing.

Data transformation and standardization.

Database modeling.

Data Warehouse implementation.

Historical data management with SCD Type 2.

ETL execution monitoring.

Docker-based development environments.

The current implementation focuses on demonstrating these concepts in a local environment. Additional security, automated testing, observability and infrastructure improvements would be necessary before production deployment.

Author

Deyvid Linoba

Data Engineering | Business Intelligence | Data Analytics

GitHub: @dlinoba

LinkedIn: deyvid-linoba