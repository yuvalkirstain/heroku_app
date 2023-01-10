import os
import psycopg2

DATABASE_URL = os.environ['DATABASE_URL']


def get_num_rows(table_name: str) -> int:
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    num_rows = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return num_rows
