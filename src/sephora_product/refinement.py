import os
import sys
import json
from datetime import datetime

import pymysql
import requests
from tqdm.auto import tqdm

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)
    
from database.access import AccessDatabase
from crawling.crawler import json_iterator

# sephora_refinement 테이블 클래스, 각 제품 별 어떤 특징 이 있는지 수집하는 부분, sephora_category_box의 내용 참고
# 각 특징 별 어떤 상품이 걸리는지 수집하는 클래스
class Refinement():
    def __init__(self):
        self.__conn__()
        
    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()

    def get_refinement_data(self, vertical):
        sql = f"select * from sephora_category_box where mid_category = \"{vertical}\";"
        self.ds_cur.execute(sql)
        data = self.ds_cur.fetchall()
        return data
    
    def get_remain_refine_product(self, vertical):
        sql = f'select product_code, refinement_name from sephora_refinement where mid_category = \"{vertical}\";'
        self.ds_cur.execute(sql)
        data = self.ds_cur.fetchall()
        product_list = {dt['product_code']: [] for dt in data}
        for dt in data:
            product_list[dt['product_code']].append(dt['refinement_name'])
        return product_list

    def get_product_all(self, refinements, vertical):
        product_list = []
        remain_product = self.get_remain_refine_product(vertical)
        for refinement in refinements:
            cat = refinement['cat_id']
            main_category = refinement['main_category']
            mid_category = refinement['mid_category']
            sub_category = refinement['sub_category']
            ref_category = refinement['display_name']
            ref_name = refinement['refinement_name']
            ref = refinement['refinement_id']
            # print(cat, ref)
            url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?ref={ref}&currentPage=1&pageSize=60&content=true&includeRegionsMap=true'
            # total_res = response.json()
            # total_product = total_res.get('totalProducts')
            res_data = json_iterator(url)
            # total_product = response.json().get('totalProducts')
            if res_data is None:
                continue
            total_product = res_data['totalProducts']
            last_page = int(total_product / 60) + 2
            for page in range(1, last_page):
                # print("ref : ", ref, "cat : ", cat, "page : " , page)
                url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?ref={ref}&currentPage={page}&pageSize=60&content=true&includeRegionsMap=true'
                # res = requests.get(url, headers=self.get_headers()).json()
                # products = res.get('products')
                res_data = json_iterator(url)
                if res_data is None:
                    break
                try:
                    products = res_data['products']
                except KeyError:
                    products = []
                for product in products:
                    product_code = product['productId']
                    remain_check = remain_product.get(product_code, [])
                    if remain_check:
                        if ref_name not in remain_check:
                            product_list.append(
                                (main_category, mid_category, sub_category, product_code, ref_category, ref_name))
                        else:
                            continue
                    else:
                        product_list.append(
                            (main_category, mid_category, sub_category, product_code, ref_category, ref_name))
        return product_list
    
    def _insert(self, data):
        query = '''
                insert into 
                sephora_refinement(main_category, mid_category, sub_category, product_code, refinement_category, refinement_name)
                values(%s, %s, %s, %s, %s, %s)
                '''
        self.ds_cur.executemany(query, data)
    
    def update_refinement(self):
        verticals = ['Eye', 'Face', 'Lip', 'Moisturizers', 'Cheek', 'Brush & Applicators', 'Accessories',
                    'Treatments', 'Masks', 'Eye Care', 'Body Care', "Men's Bath & Body", 'Wellness',
                    'Hair Styling', 'Curly Hair Care', 'Hair Dye & Root Touch-Ups', 'Shampoo & Conditioner',
                    'Hair Treatments', 'Bath & Shower', 'Cleansers', 'fragrance women', 'fragrance men',
                    'candles & home scents', 'sun care (sunscreen)', 'self tanner', 'Makeup Brushes & Applicators',
                    'Accessories', 'Nail polish & Treatments', 'Hair tools']
        products = []
        for vertical in tqdm(verticals):
            categories = self.get_refinement_data(vertical)
            products += self.get_product_all(categories, vertical)
        
        self._insert(products)
        self.__close__()
        return products