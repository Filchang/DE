import psycopg2
import csv
from datetime import datetime

def export_to_csv(conn_params, output_file):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dm.dm_f101_round_f ORDER BY from_date, to_date, ledger_account, characteristic")
        colnames = [desc[0] for desc in cursor.description]

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            for row in cursor:
                writer.writerow(row)

        print(f"[{datetime.now()}] Экспорт данных выполнен в файл: {output_file}")

    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при экспорте: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    conn_params = {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'de',
        'user': 'postgres',
        'password': 'postgres'
    }
    output_file = 'dm_f101_round_f_export.csv'
    export_to_csv(conn_params, output_file)
