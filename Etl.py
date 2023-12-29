import configparser
import mysql.connector
import pyodbc
import pypyodbc as odbc
from mysql.connector import Error

DRIVER_NAME ='SQL SERVER'
SERVER_NAME = 'DESKTOP-FC9LTU4\SQLEXPRESS'
DATABASE_NAME= 'AdventureWorks2019'


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


# Example of how to use the function for MSSQL
mssql_config = read_db_config(section='mssql')
print(mssql_config)

# Example of how to use the function for MySQL
mysql_config = read_db_config(section='mysql')
print(mysql_config)
print()


def connect():
    try:
        # Read database configurations
        db_config_mysql = mysql_config
        db_config_mssql = mssql_config


        # Establish connections
        connection_mysql = mysql.connector.connect(**db_config_mysql)

        # Simplified connection string for MSSQL
        connection_string_mssql = (
            f'DRIVER={{{db_config_mssql["driver"]}}};'
            f'SERVER={db_config_mssql["server"]};'
            f'DATABASE={db_config_mssql["database"]};'

            f'Trust_Connection=yes;'

        )
        print(connection_string_mssql)
        connection_mssql = pyodbc.connect(connection_string_mssql)

        if connection_mysql.is_connected() and connection_mssql:
            print(f'Connected to MySQL database: {db_config_mysql["database"]}')
            print(f'Connected to MSSQL database: {db_config_mssql["database"]}')

            return connection_mysql, connection_mssql

    except Error as e:
        print(f'Error: {e}')
        return None, None


# Example of how to use the connection function
connection_mysql, connection_mssql = connect()
