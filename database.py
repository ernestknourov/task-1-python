import pymysql
import click
from config import host, user, password, dtb_name, port
import json
import dicttoxml


@click.command()
@click.argument('students_path')
@click.argument('rooms_path')
@click.argument('file_format')
@click.argument('query')
@click.option('--index', help="Add indexes to sql tables(True/False)")
def main(students_path, rooms_path, file_format, query, index=False):
    """
    This is script for executing one of forth selected query's
    and saving the result into 'json' or 'xml' file.

    Query's:

    1) the list of rooms and the number of students in each of them.

    2) top 5 rooms with the smallest average age of students.

    3) top 5 rooms with the biggest difference in the age of students.

    4) a list of rooms where students of different sexes live.

    """
    # connect to database
    connection = create_connection(host, user, password, dtb_name, port)
    # clear tables for new data
    clear_tables(connection)
    # load data from files into tables
    load_data(connection, rooms_path, 'rooms')
    load_data(connection, students_path, 'students')
    # get result data
    output_data = execute_selected_query(connection, int(query))
    # add indexes (optional)
    add_indexes(connection, index)
    # output result data to the file
    output_result(file_format, output_data)
    # delete indexes
    delete_indexes(connection, index)
    print(10*"-"+"Script completed"+10*"-")


def create_connection(db_host, db_user, db_password, db_name, db_port):
    connection = None
    try:
        connection = pymysql.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("successfully connected...")
        print("#" * 20)
    except Exception as ex:
        print("Connection refused...")
        print(ex)

    return connection


def execute_query(connection, query):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
            # print("Query executed successfully")
            return cursor.fetchall()
    except Exception as e:
        print(f"The error '{e}' occurred")


def execute_selected_query(connection, query_idx):
    query1 = """SELECT rooms.name, COUNT(*) AS Amount 
                       FROM rooms, students 
                       WHERE rooms.id = students.room 
                       GROUP BY rooms.name 
                       ORDER BY rooms.id;"""
    query2 = """SELECT rooms.name, ROUND(AVG(YEAR(CURRENT_DATE)-YEAR(birthday))) AS average_age
                       FROM rooms, students
                       WHERE rooms.id = students.room
                       GROUP BY rooms.name
                       ORDER BY 2 ASC
                       LIMIT 5;"""
    query3 = """SELECT rooms.name, MAX(2022 - YEAR(birthday)) - MIN(2022 - YEAR(birthday)) AS age_difference
                       FROM rooms, students
                       WHERE rooms.id = students.room
                       GROUP BY rooms.name
                       ORDER BY 2 DESC
                       LIMIT 5;"""
    query4 = """SELECT rooms.name
                       FROM rooms, students
                       WHERE rooms.id = students.room
                       GROUP BY rooms.name
                       HAVING COUNT(DISTINCT(sex)) = 2
                       ORDER BY rooms.id"""
    queries = [query1, query2, query3, query4]
    return execute_query(connection, queries[query_idx-1])


def clear_tables(connection):
    """clear data in tables"""
    query = """DELETE FROM rooms;"""
    execute_query(connection, query)
    query = """DELETE FROM students;"""
    execute_query(connection, query)


def add_indexes(connection, condition):
    """adds indexes (upon user decision)"""
    if condition:
        query = """CREATE INDEX students_idx ON students(room,birthday,sex);"""
        execute_query(connection, query)
        query = """CREATE INDEX rooms_idx ON rooms(id);"""
        execute_query(connection, query)


def delete_indexes(connection, condition):
    """delete indexes"""
    if condition:
        query = """DROP INDEX students_idx ON students;"""
        execute_query(connection, query)
        query = """DROP INDEX rooms_idx ON rooms;"""
        execute_query(connection, query)


def load_data(connection, file_path, table_name):
    """load data from a file into a table"""
    with open(file_path) as f:
        data = json.load(f)
        for row in data:
            keys = f'{list(row.keys())}'
            keys = keys[1:-1].replace("'", "")
            values = f'{list(row.values())}'
            values = values[1:-1]
            insert_query = f"""INSERT INTO {table_name} ({keys}) VALUES ({values});"""
            execute_query(connection, insert_query)


def output_result(file_format, data):
    """output result data"""
    if file_format == 'json':
        with open('result.json', 'w') as f:
            final_data = json.dumps(data, indent=4, default=str)
            f.write(final_data)
    else:
        final_data = dicttoxml.dicttoxml(data)
        with open('result.xml', 'wb') as f:
            f.write(final_data)


if __name__ == "__main__":
    main()
