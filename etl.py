import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    This function when called loads data residing in S3 bucket to staging tables in redshift.
    :param cur: cursor object.
    :param conn: postgres connection object.
    :return: None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    This function inserts record from staging tables in redshift to the analytics tables in redshift.
    :param cur: cursor object.
    :param conn: postgres connection object.
    :return: None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    print("load staging")
    load_staging_tables(cur, conn)
    print("insert tables")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
