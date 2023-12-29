import configparser
import pypyodbc as odbc
import pandas as pd

def read_db_config(filename='config.ini', section='mssql'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return db_config

#Step 1 Import Dataset from CSV
df = pd.read_csv('C:\db\Real-Time_Traffic_Incident_Reports_20231228.csv')

#Step 2.1 Data Clean Up
df['Published Date'] = pd.to_datetime(df['Published Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['Status Date'] = pd.to_datetime(df['Status Date']).dt.strftime('%Y-%m-%d %H:%M:%S')

df.drop(df.query('Location.isnull() | Status.isnull()').index, inplace=True)

#Step 2.2 Specify Columns want to import

columns = ['Traffic Report ID', 'Published Date', 'Issue Reported', 'Location',
            'Address', 'Status', 'Status Date']

df_data = df[columns]
records = df_data.values.tolist()

#Step3.1 create connection String

mssql_config = read_db_config(section='mssql')
connection_string = f"""
            DRIVER={{{mssql_config["driver"]}}};
            SERVER={mssql_config["server"]};
            DATABASE={mssql_config["database"]};
            Trust_Connection=yes;
"""

#Step 3.2 Creaate connection instance
try:
    conn = odbc.connect(connection_string)
except odbc.DatabaseError as e:
    print('DB Error')
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error')
    print(str(e.value[1]))

sql_insert='''
    INSERT INTO Austin_Traffic_Incident 
    VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
'''
#Step 3.3 Create a cursor connection
try:
    cursor = conn.cursor()
    cursor.executemany(sql_insert, records)
    cursor.commit()
except Exception as e:
    print(str(e[1]))
finally:
    print('Task is complete')
    cursor.close()
    conn.close()
