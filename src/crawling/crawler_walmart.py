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
today = datetime.today().strftime('%y%m%d')

db_glamai = AccessDatabase('glamai')
db_jangho = AccessDatabase('jangho')

def get_brands():
    query = 'SELECT DISTINCT(`brand`) FROM `glamai_data`'
    conn, curs = db_jangho._connect()
    curs.execute(query)
    data = curs.fetchall()
    brands = pd.DataFrame(data).brand.tolist()
    curs.close()
    conn.close()
    
    return brands

def crwaling_urls(wd):
    ''' Crawling product url by brand name '''
    
    soup = BeautifulSoup(wd.page_source, 'lxml')
    products = soup.find_all('div', 'mb1 ph1 pa0-xl bb b--near-white w-25')
    urls = []
    for product in products:
        url_a = product.find('a')
        if url_a is None:
            url = None
        else:
            url = url_a['href']
        
        if 'https://www.walmart.com' in url:
            pass
        else:
            url = 'https://www.walmart.com' + url
        urls.append(url)
        
    wd.quit()
    return urls

tags = {
    'product_name': {
        'tag': 'h1',
        'attr': {'class': 'f3 b lh-copy dark-gray mt1 mb2'},
    },
    'brand_name': {
        'tag': 'a',
        'attr': {'class': 'f6 mid-gray lh-title'},
    },
    'categories': {
        'tag': 'li',
        'attr': {'class': 'w_Ba'},
    },
    'price': {
        'tag': 'span',
        'attr': {'class', 'inline-flex flex-column'},
    },
    'price_old': {
        'tag': 'span',
        'attr': {'class', 'mr2 f6 gray strike'},
    },
}


def scraping_find(target, soup):
    
    # product name
    _target = tags[target]
    _target_ = soup.find(_target['tag'], _target['attr'])
    if _target_ is None:
        _target_ = None
    else:
        _target_ = _target_.text.strip()
        
    return _target_

def scraping_find_all(target, soup):
    _target = tags[target]
    targets = soup.find_all(_target['tag'], _target['attr'])
    _target_ = []
    for t in targets:
        _target_.append(t.text.strip())
        
    if len(_target_) == 0:
        _target_ = None
    else:
        _target_ = str(_target_)
        
    return _target_


if __name__ == '__main__':
    brands = get_brands()
    urls = []
    for brand in tqdm(brands):
        brand = brand.lower().replace(' ', '-')
        url = f'https://www.walmart.com/c/brand/{brand}'
        wd = get_url(url, True, True)
        time.sleep(2.5)
        urls += crwaling_urls(wd)

    urls = list(set(urls))    
    url_df = pd.DataFrame({'url': urls})
    db_jangho.engine_upload(upload_df=url_df, table_name='walmart_urls', if_exists_option='replace')
    
    time.sleep(100)
    
    walmart_data = []
    for url in tqdm(urls):
        
        wd = get_url(url, True, True)
        time.sleep(1.5)
        soup = BeautifulSoup(wd.page_source, 'lxml')
        
        product_name = scraping_find('product_name', soup)
        brand_name = scraping_find('brand_name', soup)
        price = scraping_find('price', soup)
        price_old = scraping_find('price_old', soup)
        categories = scraping_find_all('categories', soup)
        walmart_data.append([product_name, brand_name, url, price, price_old, categories])
        
        wd.quit()
        
    walmart_df = pd.DataFrame(walmart_data, columns=['product_name', 'brand', 'url', 'price_now', 'price_old', 'categories'])
    db_jangho.engine_upload(upload_df=walmart_df, table_name='walmart_data_test', if_exists_option='replace')