import json
from datetime import datetime, timedelta
from scripts.helper_functions import common
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor


# local config
unload_dota_heroes ='./scripts/sql/unload_filtered_dota_heroes.sql'
temp_filtered_dota_heroes = '/temp/temp_filtered_dota_heroes.csv'
dota_draft_local = '/data/dota_data.csv'
dota_draft_clean_emr_steps = './dags/scripts/emr/clean_dota_draft.json'
draft_transforms_script_local = './dags/scripts/spark/draft_transforms.py'

# remote config
BUCKET_NAME = '''<redacted>'''
EMR_ID = '''<redacted>'''
temp_filtered_dota_heroes_key = 'dota_heroes/stage/{{ ds }}/temp_filtered_dota_heroes.csv'
dota_draft_load = 'dotadraft/load/dota_draft.csv'
dota_draft_load_folder = 'dotadraft/load/'
dota_draft_stage = 'dotadraft/stage/'
draft_transforms_script = 'scripts/draft_transforms.py'
get_dota_draft_report_rs = 'scripts/sql/get_dota_draft_report.sql'

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "start_date": datetime(2020, 4, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG("dota_draft_ETL_dag", default_args=default_args, schedule_interval="0 0 * * *", max_active_runs=1)

pg_unload = PostgresOperator(
    dag=dag,
    task_id='pg_unload',
    sql=unload_dota_heroes,
    mysql_conn_id='pg_default',
    params={'temp_filtered_dota_heroes': temp_filtered_dota_heroes},
    depends_on_past=True,
    wait_for_downstream=True
)

dota_heroes_to_s3_stage = PythonOperator(
    dag=dag,
    task_id='dota_heroes_to_s3_stage',
    python_callable=common.local_to_s3,
    op_kwargs={
        'filename': temp_filtered_dota_heroes,
        'key': temp_filtered_dota_heroes_key,
    },
)

remove_local_dota_heroes_file = PythonOperator(
    dag=dag,
    task_id='remove_local_dota_heroes_file',
    python_callable=common.remove_local_file,
    op_kwargs={
        'filelocation': temp_filtered_dota_heroes,
    },
)

dota_draft_to_s3_stage = PythonOperator(
    dag=dag,
    task_id='dota_draft_to_s3_stage',
    python_callable=common.local_to_s3,
    op_kwargs={
        'filename': dota_draft_local,
        'key': dota_draft_load,
    },
)

emr_script_to_s3 = PythonOperator(
    dag=dag,
    task_id='emr_script_to_s3',
    python_callable=common.local_to_s3,
    op_kwargs={
        'filename': draft_transforms_script_local,
            'key': 'scripts/draft_transforms.py',
    },
)

with open(dota_draft_clean_emr_steps) as json_file:
    emr_steps = json.load(json_file)

add_emr_steps = EmrAddStepsOperator(
    dag=dag,
    task_id='add_emr_steps',
    job_flow_id=EMR_ID,
    aws_conn_id='aws_default',
    steps=emr_steps,
    params={
        'BUCKET_NAME': BUCKET_NAME,
        'dota_draft_load': dota_draft_load_folder,
        'draft_transforms_script': draft_transforms_script,
        'dota_draft_stage': dota_draft_stage
    },
    depends_on_past=True
)

last_step = len(emr_steps) - 1

clean_dota_draft_data = EmrStepSensor(
    dag=dag,
    task_id='clean_dota_draft_data',
    job_flow_id=EMR_ID,
    step_id='{{ task_instance.xcom_pull("add_emr_steps", key="return_value")[' + str(last_step) + '] }}',
    depends_on_past=True
)

dota_heroes_to_rs_stage = PythonOperator(
    dag=dag,
    task_id='dota_heroes_to_rs_stage',
    python_callable=common.run_redshift_external_query,
    op_kwargs={
        'qry': "alter table spectrum.dota_heroes_staging add partition(insert_date='{{ ds }}') \
            location 's3://airflow-project-test/dota_heroes/stage/{{ ds }}'",
    },
)

get_dota_draft_report = PostgresOperator(
    dag=dag,
    task_id='get_dota_draft_report',
    sql=get_dota_draft_report_rs,
    postgres_conn_id='redshift'
)

pg_unload >> dota_heroes_to_s3_stage >> remove_local_dota_heroes_file >> dota_heroes_to_rs_stage
[emr_script_to_s3, dota_draft_to_s3_stage] >> add_emr_steps >> clean_dota_draft_data
[dota_heroes_to_rs_stage, clean_dota_draft_data] >> get_dota_draft_report
