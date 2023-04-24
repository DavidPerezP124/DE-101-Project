from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import nikescrapi as nk
import pandas as pd
import csv
import os

jdbc_path = '/opt/airflow/code/postgresql-42.5.4.jar,/opt/airflow/code/snowflake-jdbc-3.13.4.jar,/opt/airflow/code/spark-snowflake_2.12-2.10.0-spark_3.0.jar'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

def extract_op(path: str):
    if os.path.exists(path):
        print('Data is in memory')
    else:
        nikeAPI = nk.NikeScrAPI(max_pages=300)
        df = nikeAPI.getData()
        df.fillna(0)
        df.to_csv(path,sep="|")
    return path

with DAG('david_dag', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_op,
         op_kwargs={
            'path': '/opt/airflow/data/preprocessed-data.csv'
        }
    )

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/opt/airflow/dags/scripts/transform.py",
        conn_id="spark_conn",
        application_args=['/opt/airflow/data/preprocessed-data.csv', 'nike_raw'],
        jars=jdbc_path,
        verbose=False
    )

    load = SparkSubmitOperator(
        task_id="load",
        application="/opt/airflow/dags/scripts/load.py",
        conn_id="spark_conn",
        application_args=['--target-table','nike_raw','--snowflake-url', Variable.get("SF_URL"),
                      '--snowflake-user', Variable.get("SF_USER"),
                      '--snowflake-password', Variable.get("SF_PASS"),
                      '--snowflake-database', Variable.get("SF_DB"),
                      '--snowflake-schema', Variable.get("SF_SCHEMA"),
                      '--snowflake-warehouse', Variable.get("SF_WAREHOUSE")],
        jars=jdbc_path,
        verbose=False
    )

extract >> transform >> load
