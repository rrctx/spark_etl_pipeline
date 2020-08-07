from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import os
import logging
import psycopg2
BUCKET_NAME = "airflow-project-test"


def local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name,
                 replace=True, key=key)


def remove_local_file(filelocation):
    if os.path.isfile(filelocation):
        os.remove(filelocation)
    else:
        logging.info(f'File {filelocation} not found')


def run_redshift_external_query(qry):
    rs_hook = PostgresHook(postgres_conn_id='redshift')
    rs_conn = rs_hook.get_conn()
    rs_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    rs_cursor = rs_conn.cursor()
    rs_cursor.execute(qry)
    rs_cursor.close()
    rs_conn.commit()
