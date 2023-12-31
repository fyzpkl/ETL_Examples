import sys # System Variable

from read import read_table
def main():

    data, column_name = read_table('teams')
    print(column_name)
    for rec in data:
        print(rec)



if __name__=='__main__':
    main()