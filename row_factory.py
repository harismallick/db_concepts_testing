import datetime
import os
import psycopg
from psycopg.rows import dict_row, class_row
from dotenv import load_dotenv 
from Person import Person

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

def read_data(conn: psycopg.Connection, id: int) -> Person:
    """
    Using dict_row or class_row functions in the psycopg.rows module to return a dictionary or class object.
    This simplifies the process of inistantiating a class object from a database response.
    Both implementations are shown below.

    This method of instantiating a class object from a database response only works if no new columns are added to the table.
    If new columns are added, the class object will not be able to be instantiated, until the class is updated to include the new columns.
    Also, the class attribute names need to exactly match the column names in the database table. Otherwise it will not work.
    """
    # with conn.cursor(row_factory=dict_row) as cursor:
        # pid: int = response["id"]
        # age: int = response["age"]
        # date: datetime.date = response["dob"]
        # gender: str = response["gender"]
        # # print(f"ID: {pid}, Age: {age}, DoB: {date}, gender: {gender}")
        # person: Person = Person(pid, age, date, gender)
    # return person
    with conn.cursor(row_factory=class_row(Person)) as cursor:
        query: str = "SELECT * FROM person WHERE id = %s;"
        cursor.execute(query, (id,))
        response = cursor.fetchone()
        # print(response)
    return response

if __name__ == '__main__':
    connection = db_open_connection()
    person: Person = read_data(connection, 1)
    print(person)