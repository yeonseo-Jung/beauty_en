import re
import os
import sys
import json
import requests
from datetime import datetime
from tqdm.auto import tqdm
import pandas as pd
from user_agent import generate_user_agent

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)
    
from database.conn import AccessDatabase
from crawling.crawler import get_headers
db_glamai = AccessDatabase('glamai')

def get_status(url, item_no):
    item_no = str(item_no)
    request_data = requests.get(url, headers=get_headers())
    request_text = request_data.text
    is_use = request_text.find(item_no)
    price = None
    if is_use == -1:
        pass
    else:
        is_use = -1
        pattern = r'<script id="linkStore" type="text/json" data-comp="PageJSON ">(.*})</script>'
        target = re.findall(pattern, request_text)
        if len(target) == 0:
            pass
        else:
            result = json.loads(target[0])
            if result is None:
                pass
            else:
                try:
                    product_data = json.loads(result['page']['product']['productSeoJsonLd'])['offers']
                except Exception as e:
                    print(f'\n\n\tError Message: {e}\n\turl: {url}\n\titem_no: {item_no}')
                    product_data = []
                for product in product_data:
                    if str(product['sku']) == item_no:
                        if product['availability'] == 'http://schema.org/OutOfStock':
                            pass
                        else:
                            is_use = 1
                            price = float(product['price'])
                        break
    return price, is_use

def update_sephora_status(vertical):
    
    table_name = f'{vertical}_product_info'
    info_df = db_glamai.get_tbl(table_name, ['product_code', 'item_no', 'url', 'price', 'regist_date'])
    info_df_dedup = info_df.drop_duplicates(subset=['product_code', 'item_no'], keep='first')
    info_status = []
    for info in tqdm(info_df_dedup.values):
        product_code = info[0]
        item_no = info[1]
        url = info[2]
        price_org = info[3]
        regist_date = info[4]
        price, is_use = get_status(url, item_no)
        update_date = datetime.today()
        if price is None:
            price = price_org
        info_status.append([product_code, item_no, url, price, is_use, regist_date, update_date])
        
    upload_table = f'sephora_{vertical}_data_status'
    columns = ['product_code', 'item_no', 'url', 'price', 'is_use', 'regist_date', 'update_date']
    upload_df = pd.DataFrame(info_status, columns=columns)
    db_glamai.create_table(upload_df=upload_df, table_name=upload_table)
    return upload_df

if __name__ == '__main__':
    verticals = ['treatments', 'masks', 'eye_care', 'body_care', 'mens', 'fragrance_men', 'fragrance_women', 'wellness', 'cleansers', 'face_base', 'eye', 'lip_color', 'moisturizers', 'cheek']
    for vertical in tqdm(verticals):
        status_data_df = update_sephora_status(vertical)