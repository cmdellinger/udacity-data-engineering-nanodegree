import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ loads staging tables as defined in sql_queries"""
    num_of_tables = len(copy_table_queries)
    for index, query in enumerate(copy_table_queries):
        print("{} of {}".format(index+1, num_of_tables))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ loads schema tables as defined in sql_queries"""
    num_of_tables = len(insert_table_queries)
    for index, query in enumerate(insert_table_queries):
        print("{} of {}".format(index+1, num_of_tables))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("loading staging tables:")
    load_staging_tables(cur, conn)
    print("loading schema tables:")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()