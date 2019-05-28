import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: Drops all the tables in a database 

    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        Empty database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Creates tables in the database 

    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        Database with tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn_str = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()