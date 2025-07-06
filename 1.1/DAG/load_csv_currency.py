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

csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'md_currency_d.csv')

def load_md_currency_d():
    start_time = datetime.now()
    count_row = 0
    error_message = None

    try:
        with open(csv_path, mode='r', encoding='cp1252', errors='replace') as f:
            df = pd.read_csv(f, sep=';')


        df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'])
        df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], errors='coerce')
        df['CURRENCY_CODE'] = df['CURRENCY_CODE'].astype(str).str.slice(0, 3)
        df['CODE_ISO_CHAR'] = df['CODE_ISO_CHAR'].astype(str).str.slice(0, 2)
        df = df.where(pd.notnull(df), None)


        conn = engine.raw_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ds.md_currency_d (
                    currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (currency_rk, data_actual_date) DO UPDATE SET
                    data_actual_end_date = EXCLUDED.data_actual_end_date,
                    currency_code = EXCLUDED.currency_code,
                    code_iso_char = EXCLUDED.code_iso_char
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
            file_name='md_currency_d',
            start_time=start_time,
            end_time=end_time,
            count_row=count_row,
            error_message=error_message
        )
        print(f'Successfully uploaded. table: md_currency_d, rows: {count_row}')

with DAG(
    'load_md_currency_d',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id='load_md_currency_d',
        python_callable=load_md_currency_d
    )
