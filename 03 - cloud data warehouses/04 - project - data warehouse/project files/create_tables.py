""" create tables
    
This script drops existing tables and creates the staging tables and
final schema for the database in Redshift as defined in `sql_queries.py`
and `README.md`.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Drops existing tables with names used in schema in `sql_queries`.
        
    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
    Returns:
        `None`: actions performed, but no return value
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creates tables for staging and star schema as defined in `sql_queries`.
        
    Args:
        cur (psycopg2 connection cursor): cursor for the database connection
        conn (psycopg2 connection): connection to database
    Returns:
        `None`: actions performed, but no return value
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser(allow_no_value=True)
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()