import time
from help_scripts.log_writer import load_data_log
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'vlad',
    'start_date': datetime(2025, 1, 1)
}

engine = create_engine('postgresql+psycopg2://ds_owner:ds@localhost:5432/de')

csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'ft_balance_f.csv')

def load_ft_balance_f():
    start_time = datetime.now()
    count_row = 0
    error_message = None

    try:
        df = pd.read_csv(csv_path, sep=';')

        df['ON_DATE'] = pd.to_datetime(df['ON_DATE'])

        conn = engine.raw_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ds.ft_balance_f (
                    on_date, account_rk, currency_rk, balance_out
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (on_date, account_rk) DO UPDATE SET
                    currency_rk = EXCLUDED.currency_rk,
                    balance_out = EXCLUDED.balance_out
            """, tuple(row))

            count_row += 1

        time.sleep(5)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        error_message = str(e)
        print(f'Error: {error_message}')
        raise

    finally:
        end_time = datetime.now()
        load_data_log(
            file_name='ft_balance_f',
            start_time=start_time,
            end_time=end_time,
            count_row=count_row,
            error_message=error_message
        )
        print(f'Successfully uploaded. table: ft_balance_f, rows: {count_row}')

with DAG(
    'load_ft_balance_f',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id='load_ft_balance_f',
        python_callable=load_ft_balance_f
    )
