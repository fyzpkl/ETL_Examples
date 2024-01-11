import sys # System Variable
from util import *
from read import read_table
from write import load_table
import os
def main():

    variable_name = 'APP_SECRET'

    # Retrieve the value of the user environment variable
    variable_value = os.environ.get(variable_name)

    if variable_value is not None:
        print(f"The value of {variable_name} is: {variable_value}")
    else:
        print(f"{variable_name} is not set as a user environment variable.")

    """"

    tables = get_tables('table_list')
    db_details = read_db_config('config.ini', 'postgresql')
    for table_name in tables['table_name']:
        data, column_name = read_table(table_name)
        load_table(db_details, 'postgresql', data, column_name, table_name)


    """

if __name__=='__main__':
    main()