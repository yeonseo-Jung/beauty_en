import os
import re
import sys
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


def get_data_amazon(url):

    # wd = get_url(url)
    wd = get_url(url)
    soup = BeautifulSoup(wd.page_source, 'lxml')
    if soup is None:
        print("soup is None")
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
                avaliability_txt = avaliability.find('span').text.strip()

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
    df_amazon = df_price[df_price.affiliate_type=='amazon']
    
    return df_amazon
    
def _crawling(value):
    product_code = value[0]
    item_no = value[1]
    affiliate_type = 'amazon'
    affiliate_url = value[3]
    affiliate_image = value[4]
    regist_date = value[9]
    
    data = get_data_amazon(affiliate_url)
    if data is None:
        pass
    else:
        data = [product_code, item_no, affiliate_type, affiliate_image, regist_date] + data
        
    return data

def _upload(data, append=False):
    columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_image', 'regist_date', 'affiliate_url', 'page_status', 'avaliability_txt', 'price', 'sale_price', 'is_sale', 'is_use', 'price_']
    crawling_df = pd.DataFrame(data, columns=columns)
    
    upload_columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_url', 'affiliate_image',  'price', 'sale_price', 'is_sale', 'is_use', 'regist_date']
    upload_df = crawling_df.loc[:, upload_columns]
    upload_df.loc[:, 'update_date'] = datetime.today()
    upload_df = upload_df.sort_values(by=['product_code', 'item_no', 'regist_date', 'update_date'], ignore_index=True)
    db_jangho.create_table(upload_df=upload_df, table_name='affiliate_price_update_amazon', append=append)
    
    return crawling_df, upload_df

def main():
    df_amazon = get_data()
    datas, error = [], []
    for value in tqdm(df_amazon.values):
        data = _crawling(value)
        if data is None:
            affiliate_url = value[3]
            error.append(affiliate_url)
        else:
            datas.append(data)
    crawling_df, upload_df = _upload(datas)
    
if __name__ == '__main__':
    main()