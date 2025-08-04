
import os
import pandas as pd
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

#for postgres
from airflow.hooks.base import BaseHook
import psycopg2


default_args = {
    'owner': 'Medha Prodduturi',
    'start_date': datetime(2025, 8, 1),
    'retries': 0
}

# Define base path to the data folder inside your dags directory
base_path = os.path.join(os.path.dirname(__file__), 'data')

# Construct full paths to the CSV files
gender_file = os.path.join(base_path, 'gender.csv')
relationships_file = os.path.join(base_path, 'relationships.csv')

def extract_data():
    gender = pd.read_csv(gender_file)
    rship = pd.read_csv(relationships_file)

    #temporary CSV snapshot for load task (suggested by perplexity)
    gender.to_csv('/tmp/tmp_gender.csv', index=False)
    rship.to_csv('/tmp/tmp_relationships.csv', index=False)


def load_data():
    conn = BaseHook.get_connection('cs779_postgres')
    conn_str = (
        f"host={conn.host} dbname={conn.schema} user={conn.login} "
        f"password={conn.password} port={conn.port}"
    )

    #loading the temporary csv files saved by extract_data
    gender = pd.read_csv('/tmp/tmp_gender.csv')
    rship = pd.read_csv('/tmp/tmp_relationships.csv')

    with psycopg2.connect(conn_str) as pg_conn:
        with pg_conn.cursor() as cursor:
            #creating affairs schema
            cursor.execute('CREATE SCHEMA IF NOT EXISTS affairs;')

            #creating gender table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS affairs.gender (
                    respondent_id INTEGER,
                    affairs INTEGER,
                    gender TEXT,
                    age INTEGER,
                    yearsmarried FLOAT,
                    children TEXT,
                    religiousness INTEGER,
                    education INTEGER,
                    occupation INTEGER,
                    rating INTEGER
                );
            """)
            cursor.execute("TRUNCATE affairs.gender;")

            # Insert gender data
            for _, row in gender.iterrows():
                cursor.execute("""
                    INSERT INTO affairs.gender (
                        respondent_id, affairs, gender, age, yearsmarried, children,
                        religiousness, education, occupation, rating)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """, tuple(row))

            # creating relationship table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS affairs.relationships (
                    respondent_id INTEGER,
                    gender TEXT,
                    relationship_type TEXT,
                    yearsmarried INTEGER,
                    children INTEGER,
                    religiousness INTEGER,
                    education TEXT,
                    occupation TEXT,
                    rating INTEGER,
                    num_affairs INTEGER,
                    narrative_text TEXT,
                    edu_occup_notes TEXT,
                    life_context TEXT,
                    relationship_summary TEXT
                );
            """)
            cursor.execute("TRUNCATE affairs.relationships;")

            # Insert relationships data
            for _, row in rship.iterrows():
                cursor.execute("""
                    INSERT INTO affairs.relationships (
                        respondent_id, gender, relationship_type, yearsmarried, children,
                        religiousness, education, occupation, rating, num_affairs,
                        narrative_text, edu_occup_notes, life_context, relationship_summary)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """, tuple(row))
            
            pg_conn.commit()

with DAG(
    dag_id = 'extract_load_data',
    default_args = default_args,
    schedule = None,
    catchup = False
) as dag:
    extract_task = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data,
    )

    load_task = PythonOperator(
        task_id = 'load_data',
        python_callable = load_data
    )

    extract_task >> load_task