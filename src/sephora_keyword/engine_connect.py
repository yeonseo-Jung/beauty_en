import os
from contextlib import contextmanager



DIR_NAME = os.path.dirname(__file__)


@contextmanager
def change_dir(destination) -> None:
    
    current_wd = os.getcwd()

    try:
        os.chdir(destination)
        yield
    finally:
        os.chdir(current_wd)


def getDBInfo(jsonFile, key):
    import json
    DBInfo = json.loads(open(jsonFile).read())
    return DBInfo[key]

with change_dir(DIR_NAME):
    mycelebsDBInfo = getDBInfo('./info/mycelebsDBInfo.json', 'base')
    DBNAME = mycelebsDBInfo['NAME']
    DBHOST = mycelebsDBInfo['HOST']
    DBUSERNAME = mycelebsDBInfo['USERNAME']
    DBPASSWORD = mycelebsDBInfo['PASSWORD']

class db_connection:
    def __init__(self, db_name, DBInfo: dict = mycelebsDBInfo, shutup=False):
        import pymysql
        self.host_url = DBInfo['HOST']
        self.user_nm = DBInfo['USERNAME']
        self.passwd = DBInfo['PASSWORD']
        self.port_num = DBInfo['PORT']
        self.db_name = db_name
        self.conn = pymysql.connect(host=self.host_url, user=self.user_nm, passwd=self.passwd,
                                    port=self.port_num, db=self.db_name, charset='utf8',
                                    cursorclass=pymysql.cursors.DictCursor)
        self.shutup = shutup

    def __enter__(self):
        
        if self.conn is not None:
            self.curs = self.conn.cursor()
            if self.shutup is False:
                print('DB Connection Successful')
            return self
        else:
            
            raise IOError("Cannot access DB File")

    def __exit__(self, e_type, e_value, tb):
        
        
        if self.shutup is False:
            print("Closing Database...")
        self.curs.close()
        self.conn.close()













def engine_upload(upload_df, db_name, table_name,
                  DBInfo=mycelebsDBInfo, append_method='append', shutup=False):
    
    import pymysql
    from sqlalchemy import create_engine
    import time

    
    host_url = DBInfo['HOST']
    user_nm = DBInfo['USERNAME']
    passwd = DBInfo['PASSWORD']
    port_num = DBInfo['PORT']

    
    
    
    ConnectionStatus = None
    while ConnectionStatus is not True:
        try:
            engine = create_engine(
                f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
            engine_conn = engine.connect()
            ConnectionStatus = True
        except pymysql.OperationalError as e:
            print(e)
            time.sleep(1)
        except Exception as e:
            print(e)
            time.sleep(1)

    
    upload_df.to_sql(table_name, engine_conn, if_exists=append_method, index=False)
    if shutup is False:
        print('Upload Completed')

    
    engine_conn.close()
    engine.dispose()


@contextmanager
def connect_db(db_name, DBInfo: dict = mycelebsDBInfo):
    from sqlalchemy import create_engine
    '''주로 DB데이터를 받아오는 역할을 하는 함수'''

    host_url = DBInfo['HOST']
    user_nm = DBInfo['USERNAME']
    passwd = DBInfo['PASSWORD']
    port_num = DBInfo['PORT']

    engine = create_engine(f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
    engine_conn = engine.connect()

    try:
        print("Connect Success!")
        yield engine_conn
    finally:
        print('Closing DataBase')
        engine_conn.close()


def create_engine_connect(table_name: object, db_name: object, DBInfo: dict = mycelebsDBInfo) -> object:
    from sqlalchemy import create_engine
    import time
    import pymysql

    host_url = DBInfo['HOST']
    user_nm = DBInfo['USERNAME']
    passwd = DBInfo['PASSWORD']
    port_num = DBInfo['PORT']

    
    
    ConnectionStatus = None
    while ConnectionStatus is not True:
        try:
            engine = create_engine(
                f'mysql+pymysql://{user_nm}:{passwd}@{host_url}:{port_num}/{db_name}?charset=utf8mb4')
            engine_conn = engine.connect()
            ConnectionStatus = True
        except pymysql.OperationalError as e:
            print(e)
            time.sleep(1)
        except Exception as e:
            print(e)
            time.sleep(1)

    return engine_conn