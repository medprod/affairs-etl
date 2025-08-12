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
            cursor.execute("SET search_path TO affairs_olap;")
            cursor.execute(f"CALL {proc_name}();")
        pg_conn.commit()

def build_dimensions():
    call_procedure("affairs_olap_dimensions")

def build_facts():
    call_procedure("affairs_olap_facts")

with DAG(
    dag_id = 'olap_facts_dims_dag',
    default_args = default_args,
    schedule = None,
    catchup=False
) as dag:
    dimensions_task = PythonOperator(
        task_id = 'build_dimensions',
        python_callable=build_dimensions
    )

    facts_task = PythonOperator(
        task_id = 'build_facts',
        python_callable=build_facts
    )

    dimensions_task >> facts_task 
