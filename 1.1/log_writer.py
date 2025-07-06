import psycopg2
from datetime import datetime

def load_data_log(
    file_name: str,
    start_time: datetime,
    end_time: datetime,
    count_row: int,
    error_message: str = None
):

    duration_sec = (end_time - start_time).total_seconds()  
    load_date = start_time.date()

    start_time_time = start_time.time()
    end_time_time = end_time.time()

    try:
        conn = psycopg2.connect(
            dbname='de',
            user='logs_owner',
            password='logs',
            host='localhost',
            port='5432'
        )

        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO logs.data_load_log (
                        file_name,
                        load_date,
                        start_time,
                        end_time,
                        duration_sec,
                        count_row,
                        error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    file_name,
                    load_date,
                    start_time_time,
                    end_time_time,
                    duration_sec,
                    count_row,
                    error_message
                ))

        print("Log was successfully added for the file: {file_name}")

    except Exception as e:
        print(" Error when writing the log: {e}")
        raise

    finally:
        if 'conn' in locals():
            conn.close()
