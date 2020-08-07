# spark_etl_pipeline
This repository contains a stripped down version of a batch ETL data pipeline using Apache Airflow, PySpark, Docker, and AWS (S3, EMR, and Redshift).

## Information

* Cloned from the [puckel airflow](https://github.com/puckel/docker-airflow) repo.
* The DAGs can be found in the 'dags' folder, and the associated SQL/PySpark scripts can be found in the directories inside.
* Setup for the Redshift Spectrum schema is in the 'setup' folder.

## Architecture

![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/README_images/architecture.png?raw=true)

Data for both the Postgres database and the CSV file were directly sourced from [OpenDota's OpenAPI](https://docs.opendota.com/).

## ETL Workflow

![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/README_images/ETLflowoperators.png?raw=true)
![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/README_images/ETLflow.png?raw=true)

The DAG workflow is as follows:
1a. Postgres data for a certain query date is loaded into the temporary Docker volume location.
1b. In parallel, data from the CSV file is loaded into an S3 loading area.
1c. In parallel, the EMR steps (JSON) are loaded into S3.
2. The Postgres data is uploaded to the S3 staging area.
3a. The temporary file is deleted from the Docker volume location.
3b. EMR steps are added to the cluster.
4a. A new partition for the DAG date is added into the Redshift Spectrum Postgres data table (the table has been set up to be partitioned on insert_date).
4b. EMR step sensor continues to check for completion; EMR uses s3-dist-cp to execute the s3 --> hdfs conversions and spark-submit for the PySpark cleaning script. The data is pivoted on pick order and stored as parquet in the S3 staging area.
5. Once the two paths have completed, a Redshift query is executed and returns a report.

## Data Examples

Postgres Data: 

![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/README_images/heroesdmexample.png?raw=true)

CSV Data:

![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/README_images/draftexample.png?raw=true)

Redshift Dota Draft Report:
![alt text](https://github.com/rrctx/spark_etl_pipeline/blob/master/README_images/dotadraftreportexample.png)

The Redshift Dota Draft Report was created through grouping by 'match_id', pivoting the 'hero_id' picks on the 'order' column, and joining each of the 'hero_id' numbers on to the Postgres id numbers so that the names could be selected.
