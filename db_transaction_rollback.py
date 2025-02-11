import os
import psycopg
from dotenv import load_dotenv 

load_dotenv()

def db_open_connection() -> psycopg.Connection:
    # Variables for db connection:
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_DATABASE = os.environ.get("DB_DATABASE")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")

    conn: psycopg.Connection = psycopg.connect(
        dbname=DB_DATABASE,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    # print(type(conn))
    # print(conn.)
    return conn

def make_new_table(conn: psycopg.Connection, table_name: str, columns: dict[str, tuple]=None) -> None:
    with conn.cursor() as cursor:
        # Create the table:
        query: str = f"CREATE TABLE IF NOT EXISTS {table_name} (ID SERIAL PRIMARY KEY, COUNTER INT);"
        cursor.execute(query)
    
    conn.commit()
    return

def read_counter(id: int, conn: psycopg.Connection=None) -> int:
    if conn is None:
        conn = db_open_connection()
    with conn.cursor() as cursor:
        # Get the row to increment counter for:
        query: str = "SELECT counter FROM rollback_test WHERE id = %s;"
        cursor.execute(query, (id,))
        response = cursor.fetchone()
        print(response)
        counter = response[0]
    return counter

def update_counter(id: int, new_count: int, conn: psycopg.Connection=None) -> None:
    connection = conn
    if conn is None:
        connection = db_open_connection()

    with connection.cursor() as cursor:
        # Get the row to increment counter for:
        query: str = "UPDATE rollback_test SET counter = %s WHERE id = %s;"
        cursor.execute(query, (new_count, id))

    if conn is None:
        connection.commit()    
    return

def create_counter(conn: psycopg.Connection=None) -> int:
    connection = conn
    if conn is None:
        connection = db_open_connection()
    id: int | None = None
    with connection.cursor() as cursor:
        query: str = "INSERT INTO rollback_test (counter) VALUES (0) RETURNING id;"
        cursor.execute(query)
        response = cursor.fetchone()
        id = response[0]
    
    if conn is None:
        connection.commit()
    
    if id:
        return id

    raise Exception("Could not create counter.")

def modulus_check(count: int) -> None:
    if count % 2 == 0:
        raise Exception("Count is even.")
    return

def main() -> None:

    conn: psycopg.Connection = db_open_connection()
    make_new_table(conn, "rollback_test")
    # counter_id: int = create_counter()
    id: int = 1
    counter_value: int = read_counter(id, conn)
    print(counter_value)
    counter_value += 1
    try:
        update_counter(id, counter_value, conn)
        modulus_check(counter_value)
        conn.commit()  
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    conn.close()
    return

def main2() -> None:
    conn: psycopg.Connection = db_open_connection()
    try:
        id: int = create_counter(conn)
        modulus_check(id)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    conn.close()
    return

if __name__ == "__main__":
    # main()
    main2()