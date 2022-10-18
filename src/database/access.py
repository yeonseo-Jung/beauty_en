# necessary
import os
import sys
import time
from datetime import datetime
import pandas as pd

# db connection 
import pymysql
import sqlalchemy
    
class AccessDatabase:
    
    def __init__(self, db_name):
        self.host_url = 'db.ds.mycelebs.com'
        self.user_name = 'yeonseosla'
        self.password = 'jys9807'
        self.db_name = db_name
        
        self.today = datetime.today().strftime('%y%m%d')
        
    def _connect(self):
        ''' db connect '''
            
        port_num = 3306
        conn = pymysql.connect(host=self.host_url, user=self.user_name, passwd=self.password, port=port_num, db=self.db_name, charset='utf8')
        curs = conn.cursor(pymysql.cursors.DictCursor)
        
        return conn, curs
    
    def _execute(self, query):
        conn, curs = self._connect()
        curs.execute(query)
        conn.commit()
        curs.close()
        conn.close()
        
    def _insert(self, table: str, fields: tuple, values: tuple) -> None:
        _fields = ''
        for field in fields:
            if _fields == '':
                _fields +=  field
            else:
                _fields += ', ' + field
        _fields_ = '(' + _fields + ')'

        conn, curs = self._connect()

        query = f"INSERT INTO `{table}`{_fields_} VALUES{str(values)};"
        curs.execute(query)

        conn.commit()
        curs.close()
        conn.close()

    def get_tbl_list(self):
        ''' db에 존재하는 모든 테이블 이름 가져오기 '''

        conn, curs = self._connect()

        # get table name list
        query = "SHOW TABLES;"
        curs.execute(query)
        tables = curs.fetchall()

        table_list = []
        for table in tables:
            tbl = list(table.values())[0]
            table_list.append(tbl)
        curs.close()
        conn.close()
        
        return table_list

    def get_tbl_columns(self, table_name):
        ''' 선택한 테이블 컬럼 가져오기 '''
        
        conn, curs = self._connect()

        # get table columns 
        query = f"SHOW FULL COLUMNS FROM {table_name};"
        curs.execute(query)
        columns = curs.fetchall()

        column_list = []
        for column in columns:
            field = column['Field']
            column_list.append(field)
        curs.close()
        conn.close()
        
        return column_list

    def get_tbl(self, table_name, columns='all'):
        ''' db에서 원하는 테이블, 컬럼 pd.DataFrame에 할당 '''
        
        if table_name in self.get_tbl_list():
            st = time.time()
            conn, curs = self._connect()
            
            if columns == 'all':
                query = f'SELECT * FROM {table_name};'
            else:
                # SELECT columns
                query = 'SELECT '
                i = 0
                for col in columns:
                    if i == 0:
                        query += f"`{col}`"
                    else:
                        query += ', ' + f"`{col}`"
                    i += 1

                # FROM table_name
                query += f' FROM {table_name};'
            curs.execute(query)
            tbl = curs.fetchall()
            df = pd.DataFrame(tbl)
            curs.close()
            conn.close()
            
            ed = time.time()
            print(f'\n\n`{table_name}` Import Time: {round(ed-st, 1)}sec')
        else:
            df = None
            print(f'\n\n`{table_name}` does not exist in db')
        
        return df
    
    def get_tbls(self, table_name_list, columns):
        ''' 
        db에서 컬럼이 같은 여러개 테이블 가져오기
        db에서 테이블 가져온 후 데이터 프레임 통합 (concat)
        '''

        df = pd.DataFrame()
        for tbl in table_name_list:
            df_ = self.get_tbl(tbl, columns)
            df_.loc[:, 'table_name'] = tbl
            df = pd.concat([df, df_])
        df = df.reset_index(drop=True)
        return df
    
    def engine_upload(self, upload_df, table_name, if_exists_option="append"):
        ''' Create Table '''
        
        port_num = 3306
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user_name}:{self.password}@{self.host_url}:{port_num}/{self.db_name}?charset=utf8')
        
        # Create table or Replace table 
        upload_df.to_sql(table_name, engine, if_exists=if_exists_option, index=False)

        engine.dispose()
        print(f'\n\nTable Upload Success: `{table_name}`')
        
    def _drop(self, table_name):
        ''' Drop Table '''
        
        query = f'drop table `{table_name}`;'
        conn, curs = self._connect()
        curs.execute(query)
        conn.commit()
        curs.close()
        conn.close()
        
        print(f'\n\n`{table_name}` is dropped successful!')
        
    def _backup(self, table_name, keep=False):
            
            conn, curs = self._connect()
            
            table_list = self.get_tbl_list()
            if table_name in table_list:
                backup_table_name = f'{table_name}_bak_{self.today}'
                
                # 백업 테이블이 이미 존재하는경우 rename
                i = 1
                while backup_table_name in table_list:
                    backup_table_name = backup_table_name + f'_{i}'
                    i += 1

                if keep:
                    query = f'CREATE TABLE {backup_table_name} SELECT * FROM {table_name};'
                else:
                    query = f'ALTER TABLE {table_name} RENAME {backup_table_name};'
                curs.execute(query)
                print(f'\n\n`{table_name}` is backuped successful!\nbackup_table_name: {backup_table_name}')
            else:
                print(f'\n\n`{table_name}` does not exist in db')
            
            conn.commit()
            curs.close()
            conn.close()
        
    def create_table(self, upload_df, table_name, append=False):
        ''' Create table '''
        
        if 'data_status' in table_name:
            vertical = table_name.replace('sephora_', '').replace('_data_status', '').strip()
        elif 'data_sale' in table_name:
            vertical = table_name.replace('sephora_', '').replace('_data_sale', '').strip()
        else:
            vertical = None
            
        if table_name == 'affiliate_price_update_ulta':
            today_date = self.today
            table_name = f'{table_name}_{today_date}'
        elif table_name == 'affiliate_price_update_amazon':
            today_date = self.today
            table_name = f'{table_name}_{today_date}'
        else:
            today_date = None
            
        query_dict = {
            'sephora_product_keyword': f"CREATE TABLE `sephora_product_keyword` (\
                                        `product_code` varchar(20),\
                                        `main_sku` int(11),\
                                        `main_category` varchar(100),\
                                        `mid_category` varchar(100),\
                                        `sub_category` varchar(100),\
                                        `is_use` tinyint(1),\
                                        `regist_date` datetime,\
                                        `update_date` datetime\
                                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            f'sephora_{vertical}_data_status': f"CREATE TABLE `sephora_{vertical}_data_status` (\
                                                `pk` int(11) unsigned NOT NULL AUTO_INCREMENT,\
                                                `product_code` varchar(20),\
                                                `item_no` int(11),\
                                                `url` varchar(255),\
                                                `price` float(7,2),\
                                                `is_use` tinyint(1),\
                                                `regist_date` datetime,\
                                                `update_date` datetime,\
                                                PRIMARY KEY (`pk`)\
                                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                                                
        f'affiliate_price_update_amazon_{today_date}': f"CREATE TABLE `affiliate_price_update_amazon_{today_date}` (\
                                                `product_code` varchar(20),\
                                                `item_no` int(11),\
                                                `affiliate_type` varchar(20),\
                                                `affiliate_url` text,\
                                                `affiliate_image` varchar(255),\
                                                `price` float(7,2),\
                                                `sale_price` float(7,2),\
                                                `is_sale` tinyint(1),\
                                                `is_use` tinyint(1),\
                                                `regist_date` datetime DEFAULT NULL,\
                                                `update_date` datetime DEFAULT NULL\
                                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                                                                                                
        f'affiliate_price_update_ulta_{today_date}': f"CREATE TABLE `affiliate_price_update_ulta_{today_date}` (\
                                                `product_code` varchar(20),\
                                                `item_no` int(11),\
                                                `affiliate_type` varchar(20),\
                                                `affiliate_url` text,\
                                                `affiliate_image` varchar(255),\
                                                `price` float(7,2),\
                                                `sale_price` float(7,2),\
                                                `is_sale` tinyint(1),\
                                                `is_use` tinyint(1),\
                                                `regist_date` datetime DEFAULT NULL,\
                                                `update_date` datetime DEFAULT NULL\
                                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
                                                    
            'glamai_youtube_urls': f"CREATE TABLE `glamai_youtube_urls` (\
                                    `gy_pk` int(11) NOT NULL AUTO_INCREMENT,\
                                    `product_code` varchar(255) DEFAULT NULL,\
                                    `thumbnail` varchar(255) DEFAULT NULL,\
                                    `title` varchar(255) DEFAULT NULL,\
                                    `yt_id` varchar(255) DEFAULT NULL,\
                                    `yt_url` varchar(255) DEFAULT NULL,\
                                    `duration` varchar(255) DEFAULT NULL,\
                                    `youtuber` varchar(255) NOT NULL DEFAULT '',\
                                    `regist_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\
                                    PRIMARY KEY (`gy_pk`),\
                                    KEY `product_code` (`product_code`)\
                                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        }
        if vertical is None:
            if table_name in list(query_dict.keys()):
                query = query_dict[table_name]
            else:
                query = None
        elif today_date is None:
            if table_name in list(query_dict.keys()):
                query = query_dict[table_name]
            else:
                query = None
        else:
            if 'data_status' in table_name:
                query = query_dict[f'sephora_{vertical}_data_status']
            elif 'data_sale' in table_name:
                query = query_dict[f'sephora_{vertical}_data_sale']
            else:
                query = None
        
        if query == None:
            print('query is None')
        else:
            table_list = self.get_tbl_list()
            conn, curs = self._connect()
            if not append:
                # backup table
                self._backup(table_name)
            
                # create table
                curs.execute(query)
            else:
                if table_name in table_list:
                    pass
                else:
                    # create table
                    curs.execute(query)
            
            # upload table
            self.engine_upload(upload_df, table_name, if_exists_option='append')
            
            # drop temporary table
            if  f'{table_name}_temp' in table_list:
                curs.execute(f'DROP TABLE {table_name}_temp;')
            
            # commit & close
            conn.commit()
            curs.close()
            conn.close()        