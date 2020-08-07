# spark_etl_pipeline
This repository contains a stripped down version of a batch ETL data pipeline using Apache Airflow, PySpark, Docker, and AWS (S3, EMR, and Redshift). 

## Information

* Cloned from the [puckel airflow](https://github.com/puckel/docker-airflow) repo.
* The DAGs can be found in the 'dags' folder, and the associated SQL/PySpark scripts can be found in the directories inside.
* Setup for the Redshift Spectrum schema is in the 'setup' folder.

## Architecture

![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/architecture.png?raw=true)
