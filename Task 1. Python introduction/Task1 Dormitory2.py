import pyodbc
from sqlalchemy import create_engine
import pandas as pd

#create database
def create_db(server, db_name):
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;'
    try:
        #connect to server
        connection = pyodbc.connect(connection_string, autocommit=True)
        print("Connection success")
        try:
            with connection.cursor() as cursor:
                # checking if the database exists and create it
                cursor.execute(f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}') CREATE DATABASE {db_name}")
                print(f"Database {db_name} created or already exists")
        finally:
            connection.close()
    except Exception as ex:
        print("Connection failed", ex)
    return

#create table
def load_json_to_db(json_path, name_table, engine):
    try:
        # read JSON-file
        df_table = pd.read_json(json_path)
        # insert into table
        df_table.to_sql(name=name_table, con=engine, if_exists='replace', index=False)
        print(f'Table {name_table} created from {json_path} ... \n')
    except Exception as ex:
        print(f"Failed to load {json_path} into table {name_table}: {ex}")

#create query to db
def sql_query (query, connection_string, db_name):
    try:
        with pyodbc.connect(connection_string) as conn:
            # Выполнение запроса и извлечение данных
            result = pd.read_sql_query(query, conn)
        print("Query executed successfully")
        return result
    except Exception as ex:
        print(f"Failed to execute query: {ex}")
        return None

#Saving
def save_res(file_type, num_query, res_query):
    try:
        if file_type == 'json':
            res_query.to_json(f'E:\\DE\\data\\query_{num_query}.json')
            file_place = f'query {num_query}.json'
        else:
            res_query.to_xml(f'E:\\DE\\data\\query_{num_query}.xml')
            file_place = f'query {num_query}.xml'
        print(f'File {file_place} saved to E:\\DE\\data\\')
    except Exception as ex:
        print(f"Failed to save result: {ex}")

#MAIN PART
#create database and connect
server = "MYCOMP\\SQLEXPRESS"
db_name = "Dorm"
create_db(server, db_name)

engine = create_engine(f'mssql+pyodbc://{server}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server')

#create table
load_json_to_db("E:\\DE\\data\\rooms.json", 'rooms', engine)
load_json_to_db("E:\\DE\\data\\students.json", 'students', engine)


# sql queries. implement, save result to file
num_query = 1
#1. list of rooms and amount of students in each of them
query = """
SELECT room, COUNT (id) AS NumberOfStudents
FROM students
GROUP BY students.room 
"""
res_query = sql_query(query, f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name}; Trusted_Connection=yes;', db_name)
# input file type
file_type = input('Please specify type of export json or xml...   ')
save_res(file_type, num_query, res_query)

num_query += 1
#2. 5 rooms with the smallest average age
query = """
SELECT TOP 5 room, AVG (DATEDIFF (YEAR, birthday, GETDATE())) AS AverageAge
FROM students
GROUP BY room
ORDER BY AverageAge
"""
res_query = sql_query(query, f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;',db_name)
# input file type
file_type = input('Please specify type of export json or xml...   ')
save_res(file_type, num_query, res_query)

num_query += 1
#3. 5 rooms with the biggest age difference
query = """
SELECT TOP 5 room, MAX (DATEDIFF (YEAR, birthday, GETDATE()))- MIN(DATEDIFF (YEAR, birthday, GETDATE())) AS AgeDiff
FROM students
GROUP BY room
ORDER BY AgeDiff DESC
"""
res_query = sql_query(query, f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;', db_name)
# input file type
file_type = input('Please specify type of export json or xml...   ')
save_res(file_type, num_query, res_query)

num_query += 1
#4. List of rooms where students of different sex
query = """
SELECT room, COUNT(DISTINCT sex) as m_w
FROM students
GROUP BY room
HAVING COUNT(DISTINCT sex) > 1
ORDER BY room
"""
res_query = sql_query(query, f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db_name};Trusted_Connection=yes;',db_name)
# input file type
file_type = input('Please specify type of export json or xml...   ')
save_res(file_type, num_query, res_query)