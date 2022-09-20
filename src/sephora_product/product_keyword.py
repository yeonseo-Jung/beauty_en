import os
import sys
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

# sephora_product_keyword 채우는 클래스, 어떤 sub_category에 어떤 제품이 있는지 수집
class ProductKeyword:
    def __init__(self):
        self.today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.today_date = datetime.today().strftime('%Y-%m-%d')
        self.__conn__()

    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()

    @staticmethod
    def get_headers():
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
        }
        return headers

    ## sephora_category_list 테이블의 모든 mid_category를 받아오는 함수
    def get_all_mid_category(self):
        self.ds_cur.execute(f'select distinct mid_category from sephora_category_list;')
        category_tuple_list = self.ds_cur.fetchall()
        category_list = []
        for cat in category_tuple_list:
            category_list.append(cat['mid_category'])
        return category_list

    def get_category_list(self, vertical):
        self.ds_cur.execute(f'select * from sephora_category_list where mid_category = \"{vertical}\";')
        refine_data = self.ds_cur.fetchall()
        self.ds_cur.execute(f'select product_code from sephora_product_keyword where mid_category = \"{vertical}\";')
        remain_product = self.ds_cur.fetchall()
        remain_product = [data['product_code'] for data in remain_product]
        return {'refine_data': refine_data, 'remain_product': remain_product}

    def get_product_all(self, categories, remain_data):
        insert_product_list = []
        update_product_list = []
        counter = 0
        for category in tqdm(categories):
            counter += 1
            # if counter % 10 == 0:
                # time.sleep(random.uniform(5, 15))
            cat = category['cat_id']
            sub_category = category['sub_category']
            url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?ref=&currentPage=1&pageSize=60&content=true&includeRegionsMap=true'
            # total_res = requests.get(url, headers=self.get_headers()).json()
            # total_product = total_res.get('totalProducts')

            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                total_product = response.json().get('totalProducts')

                if not total_product:
                    continue
                last_page = int(total_product / 60) + 2
                for page in range(1, last_page):
                    # time.sleep(random.uniform(1, 4))
                    url = f'https://www.sephora.com/api/catalog/categories/{cat}/products?ref=&currentPage={page}&pageSize=60&content=true&includeRegionsMap=true'
                    # res = requests.get(url, headers=self.get_headers()).json()
                    # products = res.get('products')

                    res = requests.get(url, headers=self.get_headers())
                    if res.status_code == 200:
                        products = res.json().get('products')
                        if not products:
                            break
                        for product in products:
                            product_code = product['productId']
                            if product_code in remain_data:
                                update_product_list.append((self.today, product_code))
                            else:
                                insert_product_list.append(
                                    (category['main_category'], category['mid_category'], sub_category,
                                     product_code, product['currentSku']['skuId'], 1, self.today,
                                     self.today))
                    else:
                        print("res error from server ; ", str(res.content))
                    
                print(category, 'done')
            else:
                print("response error from server ; ", str(response.content))
                
        return insert_product_list, update_product_list
    
    def _insert(self, work, data=None):
        
        if work == 'insert':
            query = '''
                insert into
                sephora_product_keyword(main_category, mid_category, sub_category, product_code, main_sku, is_use, regist_date, update_date)
                values(%s, %s, %s, %s, %s, %s, %s, %s)
                '''
        elif work == 'update_new':
            query = '''
                update sephora_product_keyword
                set update_date = %s, is_use = 1
                where product_code = %s
            '''
        else:
            query = None
        
        if query is None:
            print("Query is None")
        else:
            if data is None:
                self.ds_cur.execute(query)
            else:
                self.ds_cur.executemany(query, data)            
            self.ds_conn.commit()
            print(f"Successful {work} data")
            
    def dup_check(self):
        products_df = self.ds.get_tbl('sephora_product_keyword')
        subset = ['product_code', 'main_category', 'mid_category', 'sub_category', 'main_sku', 'is_use']
        products_df_dedup = products_df.drop_duplicates(subset=subset, keep='first')
        sorted_by = products_df_dedup.columns.tolist()
        products_df_dedup = products_df_dedup.sort_values(by=sorted_by, ascending=False, ignore_index=True)
        
        return products_df_dedup
    
    def update_product_keyword(self):
        
        verticals = [
            'Eye', 'Face', 'Lip', 'Moisturizers', 'Cheek', 'Brush & Applicators', 'Accessories',
            'Treatments', 'Masks', 'Eye Care', 'Body Care', "Men's Bath & Body", 'Wellness',
            'Hair Styling', 'Curly Hair Care', 'Hair Dye & Root Touch-Ups', 'Shampoo & Conditioner',
            'Hair Treatments', 'Bath & Shower', 'Cleansers', 'fragrance women', 'fragrance men',
            'candles & home scents', 'sun care (sunscreen)', 'self tanner', 'Makeup Brushes & Applicators',
            'Accessories', 'Nail polish & Treatments', 'Hair tools'
        ]

        insert_products, update_products = [], []
        for vertical in tqdm(verticals):
            categories = self.get_category_list(vertical)
            insert_product, update_product = self.get_product_all(categories['refine_data'], categories['remain_product'])
            insert_products += insert_product
            update_products += update_product
        
        
        self._insert(work='insert', data=insert_products)
        self._insert(work='update_new', data=update_products)
        upload_df = self.dup_check()
        upload_df.loc[upload_df.update_date<self.today_date, 'is_use'] = 0
        self.ds.create_table(upload_df, 'sephora_product_keyword')
        self.__close__()
        
        return upload_df