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

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

from database.access import AccessDatabase
from crawling.crawler import get_url
db_glamai = AccessDatabase('glamai')
db_jangho = AccessDatabase('jangho')


def get_data_amazon(url, wd=None):

    if wd is None:
        wd = get_url(url)
    else:
        pass
    soup = BeautifulSoup(wd.page_source, 'lxml')
    if soup is None:
        print("soup is None")
        wd.quit()
        return None
    elif soup.find('div', 'a-box a-color-offset-background') is not None:
        print("\n\nEnter the characters you see below\nSorry, we just need to make sure you're not a robot.\nFor best results, please make sure your browser is accepting cookies.\n\n")
        wd.quit()
        return None
    else:    
        price = None
        price_normal, price_sale, is_sale, is_use = 0, 0, 0, 0
        # Check page status
        if soup.find('div', {'id': 'g'}) is None:
            page_status = 1
        else:
            page_status = 0
            avaliability_txt = None
        
        if page_status == 1:
            # Check currently unavailable
            if soup.find('div', {'id': 'availability'}) is None:
                avaliability_txt = None
            else:
                avaliability = soup.find('div', {'id': 'availability'})
                avaliability_txt = avaliability.text.strip()

            # Check price
            if soup.find('div', 'a-section a-spacing-none aok-align-center') is not None:
                price_area = soup.find('div', 'a-section a-spacing-none aok-align-center')
                if price_area.find('span', 'a-offscreen') is None:
                    pass
                else:
                    is_use = 1
                    price = price_area.find('span', 'a-offscreen').text
                    price_sale = round(float(price[1:]), 2)
                    if soup.find('span', 'a-size-small a-color-secondary aok-align-center basisPrice') is None:
                        price_normal = price_sale
                    else:
                        price_area = soup.find('span', 'a-size-small a-color-secondary aok-align-center basisPrice')
                        if price_area.find('span', 'a-offscreen') is None:
                            price_normal = price_sale
                        else:
                            price = price_area.find('span', 'a-offscreen').text
                            price_normal = round(float(price[1:]), 2)   
                            if price_normal > price_sale:
                                is_sale = 1
                            elif price_normal == price_sale:
                                pass
                            else:
                                if price_normal == 0:
                                    is_sale = 0
                                    price_normal = price_sale
                                else:
                                    is_sale = 1
                                    _price_normal = price_normal
                                    price_normal = price_sale
                                    price_sale = _price_normal
                                
            elif soup.find('div', {'id': 'corePrice_desktop'}) is not None:
                price_area = soup.find('div', {'id': 'corePrice_desktop'})
                try:
                    price = price_area.find('span', 'a-price a-text-price a-size-base').find('span', 'a-offscreen').text
                    price_normal = round(float(price[1:]), 2)
                    is_use = 1
                except:
                    pass
                    
                try:
                    price = price_area.find('span', 'a-price a-text-price a-size-medium apexPriceToPay').find_all('span', 'a-offscreen')[0].text
                    price_sale = round(float(price[1:]), 2)
                    is_use = 1
                except:
                    pass
                    
                if price_normal > price_sale:
                    is_sale = 1
                elif price_normal == price_sale:
                    pass
                else:
                    if price_normal == 0:
                        is_sale = 0
                        price_normal = price_sale
                    else:
                        is_sale = 1
                        _price_normal = price_normal
                        price_normal = price_sale
                        price_sale = _price_normal
    
            else:
                is_use, is_sale = -1, -1
                
        wd.quit()
        return [url, page_status, avaliability_txt, price_normal, price_sale, is_sale, is_use, price]
    
    
def get_data():
    df_price = db_glamai.get_tbl('affiliate_price')
    df_amazon = df_price[df_price.affiliate_type=='amazon'].reset_index(drop=True)
    
    return df_amazon

def _crawling(df):
    product_code = df.product_code
    item_no = df.item_no
    affiliate_type = 'amazon'
    affiliate_url = df.affiliate_url
    affiliate_image = df.affiliate_image
    regist_date = df.regist_date
    
    crawled = get_data_amazon(affiliate_url)
    while crawled is None:
        wd = get_url(affiliate_url, window=True, image=True)
        time.sleep(100)
        crawled = get_data_amazon(affiliate_url, wd)
    
    updated = [product_code, item_no, affiliate_type, affiliate_image, regist_date] + crawled
    
    return updated

def _upload(data, append=False):
    columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_image', 'regist_date', 'affiliate_url', 'page_status', 'avaliability_txt', 'price', 'sale_price', 'is_sale', 'is_use', 'price_']
    crawling_df = pd.DataFrame(data, columns=columns)
    
    upload_columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_url', 'affiliate_image',  'price', 'sale_price', 'is_sale', 'is_use', 'regist_date']
    upload_df = crawling_df.loc[:, upload_columns]
    upload_df.loc[:, 'update_date'] = datetime.today()
    upload_df = upload_df.sort_values(by=['product_code', 'item_no', 'regist_date', 'update_date'], ignore_index=True)
    db_jangho.create_table(upload_df=upload_df, table_name='affiliate_price_update_amazon', append=append)
    
    return crawling_df, upload_df
    
def update_affiliate_amazon():
    df_amazon = get_data()
    _updated, error = [], []
    for idx in tqdm(range(len(df_amazon))):
        df = df_amazon.loc[idx]
        updated = _crawling(df)
        if updated is None:
            affiliate_url = df.affiliate_url
            error.append(affiliate_url)
        else:
            _updated.append(updated)
            
    crawling_df, upload_df = _upload(_updated)

    return crawling_df, upload_df
    
if __name__ == '__main__':
    crawling_df, upload_df = update_affiliate_amazon()