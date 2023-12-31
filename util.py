import pandas as pd
import configparser
import mysql.connector
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
def get_mysql_connection(db_config_mysql):
    try:
        connection_mysql = mysql.connector.connect(**db_config_mysql)
        print('Connection Successfully Granted')
    except Error as e:
        print(f'Error: {e}')
        return None
    return connection_mysql
