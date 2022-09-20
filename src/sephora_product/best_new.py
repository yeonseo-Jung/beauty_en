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
    
from database.access import AccessDatabase


# sephora 베스트셀러 / 신규상품 랭킹 구하는 코드
class UpdateBestSellerNew:
    def __init__(self):
        self.__conn__()
        self.main_vertical_dic = {'eye': 'cat130054',
                                  'face_base': 'cat130058',
                                  'lip_color': 'cat180010',
                                  'moisturizers': 'cat1230034'}

    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()

    def truncate_table(self):
        sql = 'truncate table glamai.glamai_best_new'
        self.ds_cur.execute(sql)
        self.ds_conn.commit()

    @staticmethod
    def get_headers():
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
        }
        return headers

    def get_sort_products(self, sort, cat, page):
        if sort == 'new':
            url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?sortBy=P_BEST_SELLING%3A1%3A%3AP_RATING%3A1%3A%3AP_PROD_NAME%3A0&currentPage={page}&pageSize=60&content=true&includeRegionsMap=true'
        elif sort == 'bestselling':
            url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?sortBy=P_NEW%3A1%3A%3AP_START_DATE%3A1&currentPage={page}&pageSize=60&content=true&includeRegionsMap=true'
        else:
            url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?sortBy=P_NEW%3A1%3A%3AP_START_DATE%3A1&currentPage={page}&pageSize=60&content=true&includeRegionsMap=true'
        res = requests.get(url, headers=self.get_headers())
        return res.json()

    def get_total_page(self, total_products):
        return total_products // 60 + 2

    def insert_to_table(self, data):
        sql = '''
            insert into glamai_best_new(product_code, vertical, order_type, order_reverse, regist_date)
            values(%s, %s, %s, %s, %s)
        '''
        self.ds_cur.executemany(sql, data)
        self.ds_conn.commit()

    def update_best_new(self):
        self.truncate_table()
        today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        sort_list = ['new', 'bestselling']
        for sorts in sort_list:
            print('---------------', sorts)
            for vertical, cat in self.main_vertical_dic.items():
                print(vertical)
                first_res = self.get_sort_products(sorts, cat, 1)
                total_products = int(first_res['totalProducts'])
                total_page = self.get_total_page(total_products)
                result = []
                cnt = total_products
                for page in range(1, total_page):
                    data = self.get_sort_products(sorts, cat, page)
                    products = data['products']
                    for product in products:
                        product_data = [
                            product['productId'],
                            vertical,
                            sorts,
                            cnt,
                            today
                        ]
                        cnt -= 1
                        result.append(product_data)    
                
                self.insert_to_table(result)
        
        self.__close__()
        print("glamai_best_new 업데이트 완료")