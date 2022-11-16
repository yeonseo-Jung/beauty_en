import os
import re
import sys
import time
import json
import requests
import pickle
from tqdm import tqdm
from datetime import datetime
import numpy as np
import pandas as pd

# Scrapping
from bs4 import BeautifulSoup
from selenium import webdriver
from user_agent import generate_user_agent
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import UnexpectedAlertPresentException, NoAlertPresentException, NoSuchElementException, TimeoutException

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

from database.access import AccessDatabase
from crawling.crawler import get_url, json_iterator, get_headers
from errors import Errors
tbl_cache = os.path.join(root, 'tbl_cache')
today = datetime.today().strftime('%y%m%d')
db_glamai = AccessDatabase('glamai')
db_jangho = AccessDatabase('jangho')
errors = Errors()
_date = datetime.today().strftime("%y%m%d")
print(f'Today is {_date}')

def dup_check(df, subset):
    
    return df[df.duplicated(subset=subset, keep=False)].sort_values(by=subset, ignore_index=True)

def get_item(item):

    if 'canonicalUrl' not in item.keys():
        _item = None
    else:
        canonicalUrl = item['canonicalUrl']
        availabilityStatusDisplayValue = item['availabilityStatusDisplayValue']
        Id = item['id']
        usItemId = item['usItemId']
        name = item['name']
        price = item['price']
        wasPrice = item['priceInfo']['wasPrice'].strip()
        if wasPrice == '':
            wasPrice = float(price)
        else:
            wasPrice = re.sub(r'[$\, ]', '', wasPrice)
            wasPrice = float(wasPrice)
        rating = str(item['rating'])

        _item = [canonicalUrl, availabilityStatusDisplayValue, Id, usItemId, name, wasPrice, price, rating]
    
    return _item

def get_items(brand, end_page=25):
    
    end_page += 1
    item_cnt = 0
    itemsList = []
    for page in range(1, end_page):
        if page == 1:
            url = f'https://www.walmart.com/search?q={brand}&facet=brand:{brand}&affinityOverride=default'
        else:
            url = f'https://www.walmart.com/search?q={brand}&facet=brand:{brand}&affinityOverride=default&page={page}'

        headers = get_headers()    
        sid = 'IRfRG7tfv6ka2987617naNfNQJoqRKzUm1'
        token ='cyh5qGDZmwz8qAvdA.aaJLdBUmtpPrG-'
        response = requests.get(url, headers=headers, timeout=3.5, auth=(sid, token))
        # response = requests.post(url, headers=headers, timeout=3.5, auth=(sid, token))
        time.sleep(3.5)

        soup = BeautifulSoup(response.text, 'lxml')
        json_res = soup.find('script', {'id': '__NEXT_DATA__', 'type': 'application/json'})
        
        if json_res is None:
            items = []
        else:
            json_data = json.loads(json_res.text)
            props = json_data['props']
            pageProps = props['pageProps']
            initialData = pageProps['initialData']
            searchResult = initialData['searchResult']
            itemStacks = searchResult['itemStacks'][0]
            items = itemStacks['items']
            itemsList.append(items)
        
        # 한 페이지에 삼품 수 40개 이상일 때만 다음페이지로 이동
        if len(items) >= 40:
            item_cnt += 40
        else:
            item_cnt += len(items)
            break
        time.sleep(3.5)
    print(f'\n\n - `{brand}` items: {item_cnt}\n - pages: {page}')    
    
    return itemsList

def get_brands():
    # get brands to glamai_data
    # brands = db_jangho.get_tbl('glamai_data', ['brand']).brand.unique()
    
    df = db_jangho.get_tbl('glamai_data', ['product_code', 'brand'])
    brands_df = df.groupby('brand').count().sort_values('product_code', ascending=False)
    # # 글램아이 전체 브랜드
    brands = brands_df.index
    
    # # 개체 수 10개 이상 존재하는 브랜드 추출
    # brands_df_10 = brands_df[brands_df.product_code >= 10]
    # brands = brands_df_10.index

    preprocessed_b = []
    for brand_sephora in brands:
        _brand = brand_sephora.replace('á', 'a').replace('é', 'e').replace('ô', 'o')
        _b = []
        for b in _brand.strip().split(' '):
            _b.append(b[0].upper() + b[1:].lower())
        brand_walmart = ' '.join(_b)
        preprocessed_b.append([brand_sephora, brand_walmart])

    return preprocessed_b

def crawling_items(brands):
    
    items_dict = {}
    for brand in tqdm(brands):
        try:
            max_iters = 3 # 최대 반복 횟수
            iters = 0
            itemsList = []
            while (len(itemsList) == 0) & (iters < max_iters):
                # 두가지 케이스 존재함
                # - 월마트에 해당 브랜드가 존재하지 않음
                # - 크롤링 에러
                itemsList = get_items(brand[1])
                iters += 1
                if len(itemsList) == 0:
                    time.sleep(60)
            items_dict[brand[0]] = itemsList
        except:
            errors.errors_log()
        
    return items_dict

def scraping_items(items_dict):
    
    pk = 0
    item_df_list, variant_df_list = [], []
    for brand in items_dict.keys():
        
        itemsList = items_dict[brand]
        
        _items_ = []
        for items in tqdm(itemsList):
            _items = []
            for item in items:
                try:
                    _item = get_item(item)
                    if _item is None:
                        pass
                    else:
                        _item = [pk, brand] + _item
                        _items.append(_item)
                        
                    if 'variantList' in item.keys():
                        variantList = item['variantList']
                        if len(variantList) == 0:
                            pass
                        else:
                            df = pd.DataFrame(variantList)
                            df.loc[:, 'pk'] = pk
                            variant_df_list.append(df)
                except:
                    errors.errors_log()
                
                pk += 1
            
            _items_ += _items
                
        columns = ['pk', 'brand', 'canonicalUrl', 'availabilityStatus', 'Id', 'usItemId', 'product_name', 'price', 'sale_price', 'rating']
        df = pd.DataFrame(_items_, columns=columns)
        item_df_list.append(df)
                
    subset = 'usItemId'
    item_df = pd.concat(item_df_list).drop_duplicates(subset=subset, keep='first', ignore_index=True)
    variant_df = pd.concat(variant_df_list).rename(columns={'name': 'option'}).drop_duplicates(subset=subset, keep='first', ignore_index=True)

    return item_df, variant_df
        
        
def get_product(url, reqeusts_method=False):
    if reqeusts_method:
        sid = 'IRfRG7tfv6ka2987617naNfNQJoqRKzUm1'
        token ='cyh5qGDZmwz8qAvdA.aaJLdBUmtpPrG-'
        auth = (sid, token)
        response = requests.get(url, auth=auth)
        soup = BeautifulSoup(response.text, 'lxml')
    
    else:
        wd = get_url(url, True, False)
        time.sleep(10)
        soup = BeautifulSoup(wd.page_source, 'lxml')
        wd.quit()
    
    
    json_res = soup.find('script', {'id': '__NEXT_DATA__', 'type': 'application/json'})
    
    if json_res is None:
        status = 0
        product = None
    else:
        json_data = json.loads(json_res.text)
        

        props = json_data['props']
        pageProps = props['pageProps']
        initialData = pageProps['initialData']
        data = initialData['data']
        product = data['product']
        status = 1
    
    return status, product
        
def get_v_item(product):
    productId = product['id'] 
    usItemId = product['usItemId']
    canonicalUrl = product['canonicalUrl']
    product_name = product['name']
    brand_walmart = product['brand']
    availabilityStatus = product['availabilityStatus']    
    priceInfo = product['priceInfo']
    
    # price
    currentPrice = priceInfo['currentPrice']
    if currentPrice is None:
        pass    
    else:
        currentPrice = currentPrice['price']
    wasPrice = priceInfo['wasPrice']
    if wasPrice is None:
        wasPrice = currentPrice
    else:
        wasPrice = wasPrice['price']
    
    info = [productId, usItemId, canonicalUrl, product_name, brand_walmart, availabilityStatus, wasPrice, currentPrice]    
    columns = ['productId', 'usItemId', 'canonicalUrl', 'product_name', 'brand_walmart', 'availabilityStatus', 'price', 'sale_price']
    v_item_df = pd.DataFrame([info], columns=columns)
    
    return v_item_df

def get_variants(product):
    # scraping variant data 
    variants = product['variants']
    
    if variants is None:
        v_df = None
    else:
        variant_data = []
        for variant in variants:
            productId = variant['id'] 
            usItemId = variant['usItemId']
            availabilityStatus = variant['availabilityStatus']
            
            # price info
            priceInfo = variant['priceInfo']
            currentPrice = priceInfo['currentPrice']['price']
            wasPrice = priceInfo['wasPrice']
            if wasPrice is None:
                wasPrice = currentPrice
            else:
                wasPrice = wasPrice['price']
                
            productUrl = variant['productUrl']
            
            variant_data.append([productId, usItemId, availabilityStatus, wasPrice, currentPrice, productUrl])

        columns = ['productId', 'usItemId', 'availabilityStatus', 'price', 'sale_price', 'canonicalUrl']
        v_df = pd.DataFrame(variant_data, columns=columns)
    
    return v_df

def get_options(product):
    variantsMap = product['variantsMap']
    variantsMapKeys = variantsMap.keys() 

    variants_data = [] 
    for key in variantsMapKeys:
        val = variantsMap[key]
        usItemId = val['usItemId']
        option = val['variants']
        variants_data.append([key, usItemId, option])

    columns = ['productId', 'usItemId', 'option']    
    opt_df = pd.DataFrame(variants_data, columns=columns)
    
    return opt_df 

def crawling_options(url, pk, product_name=None, brand=None):
    # get product
    status, product = get_product(url)

    if status != 1:
        return None
    else:
        # scraping
        v_item_df = get_v_item(product)
        v_df = get_variants(product)
        if v_df is None:
            v_item_df.loc[:, 'pk'] = pk
            return v_item_df
        else:
            opt_df = get_options(product)
            v_items_df = pd.concat([v_item_df, v_df], ignore_index=True)
            v_items_df_mer = v_items_df.merge(opt_df, on=['productId', 'usItemId'], how='left')
            v_items_df_mer.loc[:, 'pk'] = pk
            return v_items_df_mer
        
        
if __name__ == '__main__':
    brands = get_brands()
    
    ## Test
    # 개체 수 상위 15개 수집
    # brands = brands[:15]
    
    items_dict = crawling_items(brands)
    item_df, variant_df = scraping_items(items_dict)
    
    try:
        db_jangho.engine_upload(upload_df=item_df, table_name=f'walmart_item_data_{_date}', if_exists_option='append')
        db_jangho.engine_upload(upload_df=variant_df, table_name=f'walmart_variant_data_{_date}', if_exists_option='append')
    except:
        errors.errors_log()
        
    path = os.path.join(tbl_cache, 'items_dict.txt')
    with open(path, 'wb') as f:
        pickle.dump(items_dict, f)
        
    v_df_dedup = variant_df.drop_duplicates('pk', ignore_index=True)
    v_df_dedup.loc[:, 'url'] = 'https://walmart.com' + v_df_dedup.loc[:, 'canonicalUrl']
    
    
    # Crawling variant data
    df_list = []
    for idx in tqdm(v_df_dedup.index):
        url = v_df_dedup.loc[idx, 'url']
        pk = v_df_dedup.loc[idx, 'pk']
        
        try:
            v_items_df = crawling_options(url, pk)
            df_list.append(v_items_df)
        except:
            errors.errors_log()
    
    concat_df = pd.concat(df_list, ignore_index=True)
    concat_df.loc[:, 'option'] = concat_df.option.astype('str')
    db_jangho.engine_upload(upload_df=concat_df, table_name=f'walmart_variant_data_price_{_date}', if_exists_option='append')