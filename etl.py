import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Extracts raw data from S3 and loads into the staging tables.
    
    Args:
        cur: The PostgreSQL/Redshift cursor object
        conn: The connection object
        
    Returns:
        None
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts data from staging area into final analytics tables in Redshift 
    
    Args:
        cur: The PostgreSQL/Redshift cursor object
        conn: The connection object
        
    Returns:
        None
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load configuration from file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish connection to Redshift
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Perform ETL
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()