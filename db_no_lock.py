import psycopg2
import psycopg2.extras
import os
import time
from dotenv import load_dotenv 
from datetime import datetime
from uuid import UUID

import psycopg2.sql

psycopg2.extras.register_uuid()

load_dotenv()

def db_open_connection() -> psycopg2.extensions.connection:
    # Variables for db connection:
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_DATABASE = os.environ.get("DB_DATABASE")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")

    conn: psycopg2.extensions.connection = psycopg2.connect(
        dbname=DB_DATABASE,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print(conn.status)
    return conn

def make_new_table(conn: psycopg2.extensions.connection, table_name: str, columns: dict[str, tuple]=None) -> None:
    with conn.cursor() as cursor:
        # Create the table:
        query: str = f"CREATE TABLE IF NOT EXISTS {table_name} (ID SERIAL PRIMARY KEY, COUNTER INT);"
        cursor.execute(query)
    
    conn.commit()
    return

def increment_counter(conn: psycopg2.extensions.connection, id: int) -> None:
    with conn.cursor() as cursor:
        # Get the row to increment counter for:
        query: str = "SELECT counter FROM test_table WHERE id = %s;"
        cursor.execute(query, (id,))
        response = cursor.fetchone()
        print(response)
        counter = response[0]
        counter += 1
        time.sleep(5)
        query2: str = "UPDATE test_table SET counter = %s WHERE id = %s;"
        cursor.execute(query2, (counter, id))
    
    conn.commit()
    return


if __name__ == '__main__':
    connection = db_open_connection()
    make_new_table(connection, "test_table")
    increment_counter(connection, 1)