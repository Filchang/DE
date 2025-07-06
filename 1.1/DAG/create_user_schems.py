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

def create_users_and_schemas():
    current_script = os.path.join(sql_dir, '01_create_user_schemas.sql')
    with open(current_script, 'r') as f:
        sql = f.read()  

    conn = psycopg2.connect(
        dbname='de',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql) 
        print('Successful creation of users and schemas')
    except Exception as e:
        print(f'Error: {e}')
        raise
    finally:
        conn.close()

def create_ds():
    current_script = os.path.join(sql_dir, '02_create_ds.sql')
    with open(current_script, 'r') as f:
        sql = f.read()
    split_sql = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]

    conn = psycopg2.connect(  
        dbname='de',
        user='ds_owner',
        password='ds',
        host='localhost',
        port='5432'
    )

    try:
        with conn:
            with conn.cursor() as cur:
                for cmd in split_sql:
                    cur.execute(cmd)
        print('Successful creation of tables in schema ds')
    except Exception as e:
        print(f'Error: {e}')
        raise
    finally:
        conn.close()

def create_logs():
    current_script = os.path.join(sql_dir, '03_create_logs.sql')
    with open(current_script, 'r') as f:
        sql = f.read()
    split_sql = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]

    conn = psycopg2.connect(
        dbname='de',
        user='logs_owner',
        password='logs',
        host='localhost',
        port='5432'
    )

    try:
        with conn:
            with conn.cursor() as cur:
                for cmd in split_sql:
                    cur.execute(cmd)
        print('Successful creation of table in schema logs')  
    except Exception as e:
        print(f'Error: {e}')
        raise
    finally:
        conn.close()

# Определение DAG
with DAG(
    dag_id='create_database',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='users_schemas',
        python_callable=create_users_and_schemas
    )

    t2 = PythonOperator(
        task_id='create_ds',
        python_callable=create_ds
    )

    t3 = PythonOperator(
        task_id='create_logs',
        python_callable=create_logs
    )

    t1 >> t2 >> t3
