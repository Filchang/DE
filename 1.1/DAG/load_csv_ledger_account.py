import time
from help_scripts.log_writer import load_data_log
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

default_args = {
    'owner': 'vlad',
    'start_date': datetime(2025, 1, 1)
}


engine = create_engine('postgresql+psycopg2://ds_owner:ds@localhost:5432/de')

csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'md_ledger_account_s.csv')


def load_md_ledger_account():

    start_time = datetime.now()
    count_row = 0
    error_message = None

    try:
        df = pd.read_csv(csv_path, sep = ';')

        df['START_DATE'] = pd.to_datetime(df['START_DATE'])
        df['END_DATE'] = pd.to_datetime(df['END_DATE'], errors='coerce')

        conn = engine.raw_connection()
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ds.md_ledger_account_s (
                    chapter,chapter_name,section_number,section_name,subsection_name,
                    ledger1_account,ledger1_account_name,ledger_account,ledger_account_name,characteristic,start_date,end_date
                ) VALUES (%s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s)
                ON CONFLICT (ledger_account, start_date) DO UPDATE SET
                    chapter = EXCLUDED.chapter,
                    chapter_name = EXCLUDED.chapter_name,
                    section_number = EXCLUDED.section_number,
                    section_name = EXCLUDED.section_name,
                    subsection_name = EXCLUDED.subsection_name,
                    ledger1_account_name = EXCLUDED.ledger1_account_name,
                    ledger_account_name = EXCLUDED.ledger_account_name,
                    characteristic = EXCLUDED.characteristic,
                    end_date = EXCLUDED.end_date
            """, tuple(row))
            count_row += 1
            
        time.sleep(5)
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print('Error: {e}')
        error_message = str(e)
        raise
    
    finally:
        end_time = datetime.now()

        load_data_log(
            file_name= 'md_ledger_account_s',
            start_time=start_time,
            end_time=end_time,
            count_row=count_row,
            error_message=error_message
        )

    print(f'Successfully uploaded. table: md_ledger_account_s')

with DAG(
    'load_md_ledger_account_s',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    load_data_task = PythonOperator(
        task_id='load_md_ledger_account_s',
        python_callable=load_md_ledger_account
    )
