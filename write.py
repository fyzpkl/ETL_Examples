from util import *



def insert_query(table_name,column_names):
    columns = ', '.join(column_names)

    value_placeholders = ', '.join(['%s' for _ in column_names])
    query = f'''
        INSERT INTO {table_name} ({columns}) VALUES ({value_placeholders})
    '''

    return query


def insert_data(connection, cursor, query, data, batch_size=100):
    records = []
    counter = 1
    for record in data:
        records.append(record)
        if counter % batch_size == 0:
            cursor.executemany(query, records)
            connection.commit()
            records = []
        counter = counter + 1
    cursor.executemany(query, records)
    connection.commit()
    return

def load_table(db_details,db_type, data, column_names, table_name):
    #db_config = read_db_config('config.ini', 'postgresql')
    connection = get_database_connection(db_details, db_type)
    cursor = connection.cursor()
    query = insert_query(table_name, column_names)
    insert_data(connection, cursor, query, data)
    connection.close()

    return
