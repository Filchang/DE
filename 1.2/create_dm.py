from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os

default_args = {
    'owner': 'vlad',
    'start_date': datetime(2025, 1, 1)
}

sql_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql')

def create_dm():
    current_script = os.path.join(sql_dir,'04_create_schemas_de_and_table.sql')
    
    with open(current_script,'r') as f:
        sql = f.read()
    
    conn = psycopg2.connect(
        dbname = 'de',
        user = 'postgres',
        password = 'postgres',
        host = 'localhost',
        port = '5432'
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                print('Succesful create schema DM and upload tables')
    except Exception as e:
        print(f'Error: {e}')
        raise
    finally:
        conn.close()
    
with DAG(
    dag_id='create_dm',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id = 'create_dm',
        python_callable = create_dm
    )

    t1
