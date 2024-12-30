import pandas as pd
from sqlalchemy import create_engine

#Параметры подключения
server = 'MYCOMP\\SQLEXPRESS'
database = 'Dormitory'

#Строка подключения
connection_string = f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

#Подключение к базе
engine = create_engine(connection_string)
print("Подключение успешно!")

df1 = pd.read_json('E:\DE\Innowise\\rooms.json')
df2 = pd.read_json('E:\DE\Innowise\students.json')

df1.to_sql('rooms', con=engine, if_exists='append', index=False)
df2.to_sql('students', con=engine, if_exists='append', index=False)
