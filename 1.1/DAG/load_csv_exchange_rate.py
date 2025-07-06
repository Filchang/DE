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

csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'md_exchange_rate_d.csv')

def load_md_exchange_rate_d():
    start_time = datetime.now()
    count_row = 0
    error_message = None

    try:
        df = pd.read_csv(csv_path, sep=';')

        df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'])
        df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], errors='coerce')

        conn = engine.raw_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ds.md_exchange_rate_d (
                    data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (data_actual_date, currency_rk) DO UPDATE SET
                    data_actual_end_date = EXCLUDED.data_actual_end_date,
                    reduced_cource = EXCLUDED.reduced_cource,
                    code_iso_num = EXCLUDED.code_iso_num
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
            file_name='md_exchange_rate_d',
            start_time=start_time,
            end_time=end_time,
            count_row=count_row,
            error_message=error_message
        )
        print(f'Successfully uploaded. table: md_exchange_rate_d, rows: {count_row}')

with DAG(
    'load_md_exchange_rate_d',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id='load_md_exchange_rate_d',
        python_callable=load_md_exchange_rate_d
    )
