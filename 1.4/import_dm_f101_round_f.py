import psycopg2
import csv
from datetime import datetime

def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = 'dm' AND table_name = 'dm_f101_round_f_v2'
        );
    """)
    exists = cursor.fetchone()[0]
    if not exists:
        print(f"[{datetime.now()}] Таблица dm.dm_f101_round_f_v2 не найдена, создаём...")
        cursor.execute("""
            CREATE TABLE dm.dm_f101_round_f_v2 AS
            SELECT * FROM dm.dm_f101_round_f WHERE false;
        """)
        conn.commit()
        print(f"[{datetime.now()}] Таблица dm.dm_f101_round_f_v2 создана.")
    cursor.close()

def change_csv(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if rows:
        rows[0]['balance_in_rub'] = str(float(rows[0]['balance_in_rub']) + 1000)

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"[{datetime.now()}] Модификация CSV завершена, сохранено в: {output_file}")

def import_from_csv(conn_params, input_file):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**conn_params)
        create_table_if_not_exists(conn)
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")

        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            placeholders = ','.join(['%s'] * len(header))
            insert_query = f"INSERT INTO dm.dm_f101_round_f_v2 ({', '.join(header)}) VALUES ({placeholders})"

            for row in reader:
                row = [None if x == '' else x for x in row]
                cursor.execute(insert_query, row)

        conn.commit()
        print(f"[{datetime.now()}] Импорт данных из {input_file} в таблицу dm.dm_f101_round_f_v2 выполнен.")

    except Exception as e:
        print(f"[{datetime.now()}] Ошибка при импорте: {e}")
        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    conn_params = {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'de',
        'user': 'postgres',
        'password': 'postgres'
    }

    export_file = 'dm_f101_round_f_export.csv'
    change_file = 'dm_f101_round_f_change.csv'

    change_csv(export_file, change_file)
    import_from_csv(conn_params, change_file)
