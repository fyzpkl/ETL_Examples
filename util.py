import pandas as pd
import configparser
import mysql.connector
import psycopg2
from mysql.connector import Error

def get_tables(path):
    tables = pd.read_csv(path, sep=':')
    return tables.query('to_be_loaded =="yes"')

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
def get_database_connection(db_config,db_type):
    try:
        if db_type == "mysql":
            connection = mysql.connector.connect(**db_config)
            print('MySQL Connection Successfully Granted')
        elif db_type == "postgresql":
            connection = psycopg2.connect(**db_config)
            print('PostgreSQL Connection Successfully Granted')
        else:
            print('Unsupported Database Type')
            return None
    except Error as e:
        print(f'Error: {e}')
        return None
    return connection
