import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Run the queries to drop any pre-existing tables from the data warehouse 
    
    Args:
        cur: The PostgreSQL/Redshift cursor object
        conn: The connection object
        
    Returns:
        None
    """      
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Run the queries to create tables in the data warehouse
    
    Args:
        cur: The PostgreSQL/Redshift cursor object
        conn: The connection object
        
    Returns:
        None
    """      
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Load configurations
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish connection to Redhshift and create the cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Run the SQL queries
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close connection
    conn.close()


if __name__ == "__main__":
    main()