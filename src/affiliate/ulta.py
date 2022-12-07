import os
import re
import sys
import time
from datetime import datetime

import pymysql
import requests
import pandas as pd
from tqdm.auto import tqdm

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

from crawling.crawler import get_url
from database.conn import AccessDatabase
db_glamai = AccessDatabase('glamai')
db_jangho = AccessDatabase('jangho')


def get_data_ulta(url):
    driver = get_url(url)
    time.sleep(5)

    big_price_txt, small_price_txt = None, None
    if driver is None:
        normal_price, sale_price = 0, 0
        is_sale, is_use = 0, 0
        status = -2
    
    else:
        # small price가 있으면 -> small price가 정상가, big price가 할인가
        # small price가 없으면 -> big price가 정상가
        is_sale, is_use = 1, 1
        status = 1
        big_price_flag = False           
        try:
            big_price_txt = driver.find_element(By.XPATH, '//*[@id="root"]/div/div/main/div/div/div[3]/div/div[3]/span[1]').text
            big_price = ''.join(x for x in big_price_txt if x not in "Price\n$").replace('Sal', '').strip()
            big_price_flag = True

            small_price_txt = driver.find_element(By.XPATH, '//*[@id="root"]/div/div/main/div/div/div[3]/div/div[3]/span[3]').text
            small_price = ''.join(x for x in small_price_txt if x not in "Original Price\n$")
            reg_drop = r'[\(\)vu]+'
            small_price = re.sub(reg_drop, '', small_price)
            
            normal_price = small_price
            sale_price = big_price

            # check sale
            normal_price = round(float(normal_price), 2)
            sale_price = round(float(sale_price), 2)
            if normal_price > sale_price:
                pass
            elif normal_price == sale_price:
                is_sale = 0
            else:
                _normal_price = normal_price
                normal_price = sale_price
                sale_price = _normal_price
            
        except NoSuchElementException:
            if big_price_flag:
                normal_price = big_price
                sale_price = big_price
                is_sale = 0
            else:
                normal_price, sale_price = 0, 0
                is_sale, is_use = 0, 0
                status = 0
            
        except Exception as e:
            print(e, url)
            normal_price, sale_price = 0, 0
            is_sale, is_use = 0, 0
            status = -1
            
    normal_price = round(float(normal_price), 2)
    sale_price = round(float(sale_price), 2)
    driver.quit()
    return status, normal_price, sale_price, is_sale, is_use, big_price_txt, small_price_txt

def get_data():
    df_price = db_glamai.get_tbl('affiliate_price')
    df_ulta = df_price[df_price.affiliate_type=='ulta']
    
    return df_ulta

def _crawling(value):
    product_code = value[0]
    item_no = value[1]
    affiliate_type = 'ulta'
    affiliate_url = value[3]
    affiliate_image = value[4]
    regist_date = value[9]
    
    status, price, sale_price, is_sale, is_use, big_price_txt, small_price_txt = get_data_ulta(affiliate_url)
    data = [product_code, item_no, affiliate_type, affiliate_url, affiliate_image, price, sale_price, is_sale, is_use, regist_date, status, big_price_txt, small_price_txt]
    
    return data

def _upload(data):
    columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_url', 'affiliate_image', 'price', 'sale_price', 'is_sale', 'is_use', 'regist_date', 'status', 'big_price', 'small_price']
    crawling_df = pd.DataFrame(data, columns=columns)
    
    upload_columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_url', 'affiliate_image',  'price', 'sale_price', 'is_sale', 'is_use', 'regist_date']
    upload_df = crawling_df.loc[:, upload_columns]
    upload_df.loc[:, 'update_date'] = datetime.today()
    upload_df = upload_df.sort_values(by=['product_code', 'item_no', 'regist_date', 'update_date'], ignore_index=True)
    db_jangho.create_table(upload_df=upload_df, table_name='affiliate_price_update_ulta')
    
    return crawling_df, upload_df

def main():
    df_ulta = get_data()
    datas, error = [], []
    for value in tqdm(df_ulta.values):
        data = _crawling(value)
        if data is None:
            affiliate_url = value[3]
            error.append(affiliate_url)
        else:
            datas.append(data)
    crawling_df, upload_df = _upload(datas)
    
if __name__ == '__main__':
    main()