'''
ETL (Extrcat transform and load) pipeline, set of functions  to load raw data from S3 to staging tables, and insert data in fact & dimension tables.
Functions:
    load_staging_tables(cur, conn)
    This function loads raw data  to staging tables.
    insert_table(cur, conn)
    This function inserts data  in fact and dimension tables.

'''
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
'''
    This function loads raw data  to staging tables.
            Parameters:
                    cur (Cursor): Cursor object to Redshift
                    conn (Connection): Connection to Redshift
            Returns:
                    No return
    
'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
'''
    This function inserts data  in fact and dimension tables.
            Parameters:
                    cur (Cursor): Cursor object to Redshift
                    conn (Connection): Connection to Redshift
            Returns:
                    No return
    
'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()