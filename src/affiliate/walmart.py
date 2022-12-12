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

from database.conn import AccessDatabase
from crawling.crawler import get_url, json_iterator, get_headers
from errors import Errors
tbl_cache = os.path.join(root, 'tbl_cache')
today = datetime.today().strftime('%y%m%d')
errors = Errors()
_date = datetime.today().strftime("%y%m%d")
print(f'Today is {_date}')

if not os.path.isdir(tbl_cache):
    os.mkdir(tbl_cache)

def init_db(database):
    return AccessDatabase(database)

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
    
    # 글램아이 전체 브랜드 가져오기
    db = init_db("glamai")
    df = db.get_tbl('glamai_data', ['product_code', 'brand'])
    brands_df = df.groupby('brand').count().sort_values('product_code', ascending=False)
    brands = brands_df.index

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
        
def update():
    brands = get_brands()
    
    # Crawling item data
    items_dict = crawling_items(brands)
    item_df, variant_df = scraping_items(items_dict)
    
    db = init_db("jangho")
    try:
        db.engine_upload(upload_df=item_df, table_name=f'walmart_item_data_{_date}', if_exists_option='append')
        db.engine_upload(upload_df=variant_df, table_name=f'walmart_variant_data_{_date}', if_exists_option='append')
    except:
        errors.errors_log()
        
    path = os.path.join(tbl_cache, 'items_dict.txt')
    with open(path, 'wb') as f:
        pickle.dump(items_dict, f)
        
    v_df_dedup = variant_df.drop_duplicates('pk', ignore_index=True)
    v_df_dedup.loc[:, 'url'] = 'https://walmart.com' + v_df_dedup.loc[:, 'canonicalUrl']
    
    time.sleep(100)
    
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
    db.engine_upload(upload_df=concat_df, table_name=f'walmart_variant_data_price_{_date}', if_exists_option='append')

import ast
from affiliate._preprocess import preprocess_titles, dup_check, subtractor

def _preprocessing():
    
    '''Glamai Tables
    - ds > jangho > `glamai_data`
    - ds > jangho > `glamai_detail_data`
    '''
    db_glamai = init_db("glamai")
    columns = ['product_code', 'item_no', 'product_name', 'brand', 'main_vertical', 'size', 'is_use']
    glamai_data = db_glamai.get_tbl('glamai_data', columns)
    columns = ['product_code', 'item_no', 'color', 'is_use']
    glamai_detail = db_glamai.get_tbl('glamai_detail_data', columns)

    glamai_df_0 = glamai_data.merge(glamai_detail.loc[:, ['product_code', 'item_no', 'color']], on=['product_code', 'item_no'], how='left')
    glamai_df_1 = glamai_data.loc[:, ['product_code', 'product_name', 'brand', 'main_vertical']].merge(glamai_detail.loc[:, ['product_code', 'item_no', 'color', 'is_use']], on=['product_code'], how='left')
    glamai_df = pd.concat([glamai_df_0, glamai_df_1]).drop_duplicates(subset=['product_code', 'item_no'], ignore_index=True)

    glamai_df.loc[glamai_df['color'].str.strip()=='', 'color'] = None
    glamai_df.loc[glamai_df['size']=='', 'size'] = None

    '''Walmart Tables
    walmart_item_data_{_date}
    walmart_variant_data_{_date}
    walmart_variant_data_price_{_date}
    '''
    db_jangho = init_db("jangho")
    _walmart_df = db_jangho.get_tbl(f'walmart_item_data_{_date}')
    _walmart_df.loc[:, 'url'] = 'https://www.walmart.com/' + _walmart_df.loc[:, 'canonicalUrl']

    subset = ['usItemId']
    walmart_df_dedup = _walmart_df.drop_duplicates(subset=subset, keep='first', ignore_index=True)
    col = ['pk', 'usItemId', 'brand', 'product_name']
    walmart_df = walmart_df_dedup.loc[:, col]
    
    preprocessed_df_0 = preprocess_titles(glamai_df)
    preprocessed_df_1 = preprocess_titles(walmart_df)
    preprocessed_df_0 = preprocessed_df_0[(preprocessed_df_0.brand.notnull()) & (preprocessed_df_0.preprocessed.notnull())].reset_index(drop=True)
    preprocessed_df_1 = preprocessed_df_1[(preprocessed_df_1.brand.notnull()) & (preprocessed_df_1.preprocessed.notnull())].reset_index(drop=True)

    df_list = []
    for idx in tqdm(preprocessed_df_0.index):
        product_code = preprocessed_df_0.loc[idx, 'product_code']
        item_no = preprocessed_df_0.loc[idx, 'item_no']
        
        color = preprocessed_df_0.loc[idx, 'color']
        size = preprocessed_df_0.loc[idx, 'size']
        title = preprocessed_df_0.loc[idx, 'preprocessed'].lower().replace(' ', '')
        brand = preprocessed_df_0.loc[idx, 'brand'].lower().replace(' ', '')
        
        # 브랜드, 타이틀 모두 일치하는 개체 찾기
        brand_mapped = preprocessed_df_1.brand.str.lower().str.replace(' ', '').str.fullmatch(brand)
        title_mapped = preprocessed_df_1.preprocessed.str.replace(' ', '').str.fullmatch(title)
        mapped_data = preprocessed_df_1[brand_mapped & title_mapped].reset_index(drop=True)
        
        if mapped_data.empty:
            pass
        else:    
            mapped_data.loc[:, 'product_code'] = product_code
            mapped_data.loc[:, 'item_no'] = item_no
            mapped_data.loc[:, 'color'] = color
            mapped_data.loc[:, 'size'] = size
            df_list.append(mapped_data)

    mapped_df = pd.concat(df_list, ignore_index=True)
    options_df = db_jangho.get_tbl(f'walmart_variant_data_price_{_date}')
    for idx in options_df.index:
        opts = ast.literal_eval(options_df.loc[idx, 'option'])
        for opt in opts:
            if opt[0:12] == 'actual_color':
                color = opt.replace('actual_color-', '').strip()
                options_df.loc[idx, 'walmart_color'] = color
            elif opt[0:4] == 'size':
                size = opt.replace('size-', '').strip()
                options_df.loc[idx, 'walmart_size'] = size
            else:
                options_df.loc[idx, 'another_option'] = opt
                
    # 대표상품 중 옵션 존재 개체 join
    opt_0 = mapped_df.merge(options_df.loc[:, ['usItemId', 'productId', 'walmart_color', 'walmart_size', 'another_option']], on='usItemId', how='left')

    # 매핑 상품에 옵션 부여
    pks = mapped_df.pk.unique() # 매핑 완료 개체 (대표상품)
    ids = opt_0[opt_0.productId.notnull()].usItemId.unique() # 대표상품 중 옵션 존재 usItemId
    col = ['pk', 'productId', 'usItemId', 'product_name', 'walmart_color', 'walmart_size', 'another_option']
    _opt_1 = options_df.loc[(options_df.pk.isin(pks)) & (options_df.usItemId.isin(ids)==False), col]

    col = ['pk', 'product_code', 'item_no', 'brand', 'color', 'size']
    _mapped_df = mapped_df.loc[:, col]
    opt_1 = _opt_1.merge(_mapped_df, on='pk', how='left')

    # concat
    mapped_opt_df = pd.concat([opt_0, opt_1]).sort_values(by=['pk', 'usItemId'], ignore_index=True)

    # convert np.nan to None
    mapped_opt_df = mapped_opt_df.where(pd.notnull(mapped_opt_df), None)

    cols = ['walmart_color', 'walmart_size', 'another_option', 'volume_oz', 'volume_ml', 'volume_kg']
    opts = ['color', 'size']
    for idx in mapped_opt_df.index:
        if mapped_opt_df.loc[idx, cols+opts].isnull().values.tolist().count(True) == len(opts) + len(cols):
            # 옵션 자체가 존재하지 않음
            status = -1       
            attrs = None
        else:
            attrs = []
            for opt in opts:    
                opt = mapped_opt_df.loc[idx, opt]
                if opt is None:
                    pass
                else:
                    opt = opt.lower().replace(' ', '')
                    for col in cols:
                        attr = mapped_opt_df.loc[idx, col]
                        if attr is None:
                            pass
                        else:    
                            attr = attr.lower().replace(' ', '')
                            if attr in opt:
                                attrs.append(attr)
            if len(attrs) == 0:
                # 옵션은 존재하지만 일치하지 않음
                status = 0
                attrs = None
            else:
                # 옵션 일치
                status = 1
                attrs = str(list(set(attrs))) 
        
        mapped_opt_df.loc[idx, 'attributes'] = attrs
        mapped_opt_df.loc[idx, 'mapped_status'] = status

    # group by mapped_status count
    print(f"\n\n{mapped_opt_df.groupby('mapped_status').count()}\n\n")
    
    # mapped_status==1: 브랜드, 상품명, 옵션 일치 
    columns = ['product_code', 'item_no', 'brand', 'pk', 'usItemId', 'productId', 'attributes']
    mapped_df_comp = mapped_opt_df.loc[mapped_opt_df.mapped_status==1, columns].sort_values(by=['product_code', 'item_no'], ignore_index=True)
    print(f'- Glamai data: {len(glamai_df)}\n- Walmart data: {len(walmart_df)}\n- Mapping data: {len(mapped_df_comp)}')
    
    # Upload table: ds > jangho > `sephora_to_walmart_mapped_{_date}`
    db_jangho.engine_upload(mapped_opt_df, f'sephora_to_walmart_mapped_{_date}', if_exists_option='replace')
    
def _mapping():

    # get data
    db_jangho = init_db("jangho")
    item_df = db_jangho.get_tbl(f'walmart_item_data_{_date}')
    v_p_df = db_jangho.get_tbl(f'walmart_variant_data_price_{_date}')
    map_df = db_jangho.get_tbl(f'sephora_to_walmart_mapped_{_date}')
    cols = ['product_code', 'item_no', 'usItemId', 'mapped_status']
    _map_df = map_df.loc[:, cols]

    cols = ['usItemId', 'canonicalUrl', 'price', 'sale_price', 'availabilityStatus']
    _item_df = item_df.loc[:, cols]
    _v_p_df = v_p_df.loc[:, cols]
    
    # Merge: price data
    mer_df_0 = _map_df.merge(_item_df, on='usItemId', how='inner')
    mer_df_1 = _map_df.merge(_v_p_df, on='usItemId', how='inner')

    mer_df = pd.concat([mer_df_0, mer_df_1], ignore_index=True)
    subset = ['product_code', 'item_no', 'usItemId']
    dedup_df = mer_df.drop_duplicates(subset=subset, keep='last', ignore_index=True)

    # affiliate data
    dedup_df.loc[:, 'affiliate_type'] = 'walmart'
    dedup_df.loc[:, 'affiliate_url'] = 'https://www.walmart.com' + dedup_df['canonicalUrl']
    dedup_df.loc[:, 'affiliate_image'] = 'https://alls3.glamai.com/images/affiliate/walmart.jpg'
    
    # stock status check (is_use)
    dedup_df.loc[(dedup_df['availabilityStatus']=='In stock') | (dedup_df['availabilityStatus']=='IN_STOCK'), 'is_use'] = True
    dedup_df.loc[(dedup_df['availabilityStatus']!='In stock') & (dedup_df['availabilityStatus']!='IN_STOCK'), 'is_use'] = False

    # sale status check (is_sale)
    dedup_df.loc[dedup_df['price']>dedup_df['sale_price'], 'is_sale'] = True
    dedup_df.loc[dedup_df['price']==dedup_df['sale_price'], 'is_sale'] = False

    dedup_df.loc[dedup_df['sale_price']==0, 'is_sale'] = False
    dedup_df.loc[dedup_df['sale_price']==0, 'is_use'] = False
    
    columns = ['product_code', 'item_no', 'affiliate_type', 'affiliate_url', 'affiliate_image', 'usItemId', 'price', 'sale_price', 'is_sale', 'is_use']
    upload_df = dedup_df.loc[dedup_df.mapped_status==1, columns].sort_values(by=['product_code', 'item_no'], ignore_index=True)
    regist_date = pd.Timestamp(datetime.today().strftime("%Y-%m-%d"))
    upload_df.loc[:, 'regist_date'] = regist_date
    upload_df.loc[:, 'update_date'] = regist_date

    """
    월마트 내부 중복 체크 
    - dedup = 1(True): 대표상품 (가격 낮은 상품)
    - dedup = 0(False): 종속상품 (가격 높은 상품 or is_use = 0(False))
    """
    _upload_df = upload_df[upload_df.is_use].sort_values(by='sale_price', ignore_index=True)
    upload_df_ = upload_df[upload_df.is_use==False]

    subset = ['product_code', 'item_no']
    dedup_df = _upload_df.drop_duplicates(subset=subset, keep='first', ignore_index=True)
    dup_df = dup_check(df=_upload_df, subset=subset, keep='first', sorting=True)

    dedup_df.loc[:, 'dedup'] = True
    if not upload_df_.empty:
        upload_df_ = upload_df_.reset_index(drop=True)
        upload_df_.loc[:, 'dedup'] = False # is_use=0(False) 이면 중복으로 간주 -> 서비스 테이블에 insert 안 되게 하기 위함
    dup_df.loc[:, 'dedup'] = False
    concat_df = pd.concat([dedup_df, upload_df_, dup_df]).sort_values(by=subset, ignore_index=True)

    print(f"\n\n{concat_df.groupby('dedup').count()}\n\n")
    
    # Table Upload: ds > jangho > sephora_to_walmart_mapped
    db_jangho = init_db("jangho")
    db_jangho.create_table(upload_df=concat_df, table_name='sephora_to_walmart_mapped')
    
    return concat_df
    
def upload():
    concat_df = _mapping()
    db_glamai = init_db("glamai")
    db_jangho = init_db("jangho")
    
    query = f'''
    update glamai.affiliate_price as a
    join jangho.sephora_to_walmart_mapped as b 
    on a.product_code = b.product_code and a.item_no = b.item_no and a.affiliate_type = b.affiliate_type
    set a.price = b.price, a.sale_price = b.sale_price, a.is_sale = b.is_sale, a.is_use = b.is_use, a.update_date = b.update_date
    where b.dedup=1;'''
    db_jangho._execute(query)
    
    query = f"""
    SELECT product_code, item_no
    FROM affiliate_price as a
    WHERE a.is_use=1 and a.affiliate_type='walmart';
    """
    data = db_glamai._execute(query)
    df = pd.DataFrame(data)
    dedup_df = concat_df.loc[concat_df.dedup, ['product_code', 'item_no', 'affiliate_type', 'affiliate_url', 'affiliate_image', 'price', 'sale_price', 'is_sale', 'is_use', 'regist_date', 'update_date']]

    subset=['product_code', 'item_no']
    new_df = subtractor(dedup_df, df, subset)
    print(f"\n\nNew product counts: {len(new_df)}\n\n")
    
    affi_tbl = "affiliate_price"

    # Table bakcup: ds > glamai > affiliate_price_bak_{_date}
    db_glamai._backup(table_name=affi_tbl, keep=True)

    # Table upload: ds > glamai > affiliate_price
    db_glamai.engine_upload(upload_df=new_df, table_name=affi_tbl, if_exists_option='append')
    
    # glamai.affiliate_price dedup & dup check
    # /* dedup query */
    dedup_query = f"""
    delete 
        t1 
    from 
        glamai.affiliate_price t1, glamai.affiliate_price t2
    where 
        t1.product_code = t2.product_code and 
        t1.item_no = t2.item_no and
        t1.affiliate_type = t2.affiliate_type and
        t1.update_date < t2.update_date;"""

    # /* dup check query */	
    dup_check_query = f"""
    select product_code, item_no, affiliate_type, count(*) cnt from glamai.affiliate_price group by product_code, item_no, affiliate_type having cnt > 1;
    """

    data = db_glamai._execute(dup_check_query)
    df = pd.DataFrame(data)
    if df.empty:
        print("중복 제거 완료.")
    else:
        print("중복인 행이 존재합니다. 중복제거 진행합니다.")
        data = db_glamai._execute(dedup_query)

if __name__ == '__main__':
    update()
    time.sleep(60)
    upload()