from util import *
def read_table(table_name,limit=0):
    db_config = read_db_config('config.ini', 'mysql')
    connection = get_mysql_connection(db_config)
    cursor = connection.cursor()
    if limit == 0:
        query = f'Select * From {table_name}'
    else:
        query = f'Select * From {table_name} LIMIT {limit}'

    cursor.execute(query)
    data = cursor.fetchall()
    column_names = cursor.column_names
    connection.close()
    return data, column_names
