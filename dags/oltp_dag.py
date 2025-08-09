import os
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2

default_args = {
    'owner': 'Medha Prodduturi',
    'start_date': datetime(2025, 8, 1),
    'retries': 0
}

def call_procedure(proc_name):
    conn = BaseHook.get_connection('cs779_postgres')
    conn_str = (
        f"host={conn.host} dbname={conn.schema} user={conn.login} "
        f"password={conn.password} port={conn.port}"
    )
    with psycopg2.connect(conn_str) as pg_conn:
        with pg_conn.cursor() as cursor:
            cursor.execute("SET search_path TO affairs;")
            cursor.execute(f"CALL {proc_name}();")
        pg_conn.commit()

def join_gender_rship():
    call_procedure("join_gender_rship")

def fill_null_values():
    call_procedure("fill_null_values")

def normalize_gender_rship():
    call_procedure("normalize_gender_rship")

with DAG(
    dag_id='oltp_normalized_dag',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    
    join_task = PythonOperator(
        task_id='join_gender_rship',
        python_callable=join_gender_rship
    )

    fill_nulls_task = PythonOperator(
        task_id='fill_null_values',
        python_callable=fill_null_values
    )

    normalize_task = PythonOperator(
        task_id='normalize_gender_rship',
        python_callable=normalize_gender_rship
    )

    join_task >> fill_nulls_task >> normalize_task

