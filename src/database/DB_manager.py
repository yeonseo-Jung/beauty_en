import pymysql
import datetime
from sqlalchemy import create_engine


host_url = 'db.ds.mycelebs.com'
username = 'yeonseosla'
passwd = 'jys9807'
port_num = 3306


def get_connection(host_name, db_name):
    conn = pymysql.connect(host=host_name,
                           user=username,
                           passwd=passwd,
                           db=db_name,
                           cursorclass=pymysql.cursors.DictCursor)
    return conn


def get_connection_to_mycelebs(db_name):
    conn = get_connection(host_url, db_name)
    return conn


def engine_upload(db_name, table_name, upload_df):
    engine = create_engine(f'mysql+pymysql://{username}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    engine_conn = engine.connect()
    upload_df.to_sql(table_name, engine_conn, if_exists='append', index=False)
    engine_conn.close()
    engine.dispose()


def truncate_table(db_name, table_name):
    conn = get_connection_to_mycelebs(db_name)
    cur = conn.cursor()
    truncate_qry = f"TRUNCATE TABLE {db_name}.{table_name};"
    cur.execute(truncate_qry)
    conn.commit()
    cur.close()
    conn.close()


def backup_table(db_name, table_name):
    backup_table_name = table_name + "_backup_" + datetime.datetime.now().strftime("%y%m%d")
    conn = get_connection_to_mycelebs(db_name)
    cur = conn.cursor()
    backup_qry = f"CREATE TABLE {backup_table_name} SELECT * FROM {table_name};"
    cur.execute(backup_qry)
    conn.commit()
    cur.close()
    conn.close()