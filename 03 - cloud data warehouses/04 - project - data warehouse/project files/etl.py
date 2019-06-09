""" ETL pipeline
    
Tells Redshift to `COPY` JSON files in `s3://udacity-dend/log_data` and
`s3://udacity-dend/song_data` trees, then transforms and inserts the information
into a Redshift database schema defined in `sql_queries.py` and `README.md`.
"""
    
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Calls `COPY` in Redshift to ingest the JSON files into staging tables.
        
    The function runs `COPY` commands to direct Redshift to ingest the JSON
    files located in the S3 bucket into the staging tables as defined in
    `sql_queries`.
    
    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
    Returns:
        `None`: actions performed, but no return value
    """
    num_of_tables = len(copy_table_queries)
    for index, query in enumerate(copy_table_queries):
        print("{} of {}".format(index+1, num_of_tables))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Extracts data from staging tables and inserts it into star schema.
    
    The function runs SQL queries defined in `sql_queries` using combination
    `INSERT` ... `SELECT` statements to read data from staging tables right into
    the schema tables.
    
    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
    Returns:
        `None`: actions performed, but no return value
    """
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