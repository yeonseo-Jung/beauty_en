import requests
import os
import sys
import re
import time
import random
import requests
from datetime import datetime

import html
import pymysql
from tqdm.auto import tqdm

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)
    
from database.conn import AccessDatabase

# sephora 비건 / 오가닉 상품 가져오는 코드
class SephoraVeganOrganic:
    def __init__(self):
        self.__conn__()
        self.vertical_dic = {"eye": 1050013, "face_base": 1050001, "lip_color": 1050024, "moisturizers": 2220432}

    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()

    def _delete(self):
        sql = 'delete from sephora_keywords where keyword_type = \'vegan_organic\';'
        self.ds_cur.execute(sql)
        self.ds_conn.commit()

    @staticmethod
    def get_headers():
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
        }
        return headers

    def get_sort_products(self, query, node, page):
        url = f'https://www.sephora.com/api/catalog/search/?type=keyword&q={query}&sortBy=P_BEST_SELLING%3A1%3A%3AP_RATING%3A1%3A%3AP_PROD_NAME%3A0&currentPage={page}&pageSize=60&node={node}&content=true&includeRegionsMap=true'
        res = requests.get(url, headers=self.get_headers())
        return res.json()

    def insert_to_table(self, data):
        sql = '''
            insert into sephora_keywords(keyword_type, product_code, id, keyword)
            values(%s, %s, %s, %s)
        '''
        self.ds_cur.executemany(sql, data)
        self.ds_conn.commit()

    def get_total_page(self, total_products):
        return total_products // 60 + 2

    def update_keywords(self):
        self._delete()
        query_list = ['vegan', 'organic']
        for query in query_list:
            print('-------------------------', query)
            for vertical, node in self.vertical_dic.items():
                print(vertical)
                url = f'https://www.sephora.com/api/catalog/search/?type=keyword&q={query}&sortBy=P_BEST_SELLING%3A1%3A%3AP_RATING%3A1%3A%3AP_PROD_NAME%3A0&currentPage=1&pageSize=60&node={node}&content=true&includeRegionsMap=true'
                res = requests.get(url, headers=self.get_headers())
                if res.status_code == 200:
                    data = res.json()
                    total_products = int(data['totalProducts'])
                    total_page = self.get_total_page(total_products)
                    result = []
                    for page in range(1, total_page):
                        url = f'https://www.sephora.com/api/catalog/search/?type=keyword&q={query}&sortBy=P_BEST_SELLING%3A1%3A%3AP_RATING%3A1%3A%3AP_PROD_NAME%3A0&currentPage={page}&pageSize=60&node={node}&content=true&includeRegionsMap=true'
                        res = requests.get(url, headers=self.get_headers())
                        if res.status_code ==200:
                            data = res.json()
                            products = data['products']
                            for product in products:
                                data = ['vegan_organic', product['productId'], 0, query]
                                result.append(data)
                        else :
                            print("error")
                            continue
                else:
                    print("error")
                    continue

                self.insert_to_table(result)

        self.__close__()
        print("sephora_keywords 업데이트 완료")