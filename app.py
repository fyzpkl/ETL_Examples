import sys
from util import get_tables
def main():
    tables = get_tables('table_list')
    for table in tables['table_name']:
        print(table)


if __name__=='__main__':
    main()