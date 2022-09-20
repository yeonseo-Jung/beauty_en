"""
sephora_all_data product 개체 생성하는 코드
first_category_uid = f"https://www.sephora.com/api/catalog/categories/cat60004/products?currentPage=1&pageSize=300&content=true&includeRegionsMap=true"
first_category_uid = f"https://www.sephora.com/api/catalog/categories/{cat_id}/products?currentPage={page_num}&pageSize=300&content=true&includeRegionsMap=true"
"""
import pandas as pd
import requests
import time
from datetime import datetime
from tqdm import tqdm
import random
from database.DB_manager import get_connection_to_mycelebs, engine_upload, truncate_table, backup_table
from json import JSONDecodeError


def get_category_product_data(cat_id="cat60004"):
    """
    sephora category 내 개체 정보 반환 api 하위 데이터 수집
    :param cat_id: sephora_category_list 의 카테고리 아이디
    :return: sephora category 내 개체 정보 반환
    """
    url = f"https://www.sephora.com/api/catalog/categories/{cat_id}/products?currentPage=1&pageSize=300&content=true&includeRegionsMap=true"
    r = requests.get(url, headers=get_header())

    try:
        total_page = r.json()['totalProducts'] // 300 + 2
    except JSONDecodeError as decode_err:
        print(decode_err)
        return url
    except Exception as e:
        if r.json()['errorCode'] == 404:
            print('error: ', r.json()['errorCode'])
            return url
        else:
            print('error: ', e)
            print('error cat_id: ', cat_id)
            return url

    result = []

    for page_num in range(1, total_page):
        url = f"https://www.sephora.com/api/catalog/categories/{cat_id}/products?currentPage={page_num}&pageSize=300&content=true&includeRegionsMap=true"
        r = requests.get(url, headers=get_header())
        try:
            data = r.json()
            result.append(data)
        except JSONDecodeError as decode_err:
            print(decode_err)
            return url

    return result


def get_category_id():
    """
    sephora category 아이디 리스트 반환
    :return: sephora category 리스트
    """
    conn = get_connection_to_mycelebs("glamai")
    category_id_data = pd.read_sql("select * from glamai.sephora_category_list", conn)
    conn.close()

    return category_id_data


def get_category_data():
    """
    카테고리 하위 개체들에 merge 할 카테고리 정보
    :return: sephora_product_keyword 테이블의 (main_category, mid_category, sub_category, product_code, main_sku) main_sku = item_no
    """
    conn = get_connection_to_mycelebs("glamai")
    category_data = pd.read_sql(f"select main_category, mid_category, sub_category, product_code, main_sku from glamai.sephora_product_keyword where mid_category not in ('eye', 'face', 'lip', 'moisturizers');", conn)
    conn.close()

    return category_data


def get_error_price_data():
    """
    수집된 sephora_all_data 에서 가격이 범위로 지정된 개체들 추출 (ex. $25.00 - $129.00)
    :return:
    """
    conn = get_connection_to_mycelebs("glamai")
    price_error_data = pd.read_sql("select * from glamai.sephora_all_data where price=0", conn)
    conn.close()

    return price_error_data


def update_result(df):
    """
    기존 데이터 백업후 sephora_all_data 테이블 truncate후 데이터 입력
    :param df: 카테고리 api에서 내려오는 개체 데이터 DF
    :return:
    """
    backup_table('glamai', 'sephora_all_data')
    truncate_table('glamai', 'sephora_all_data')
    engine_upload('glamai', "sephora_all_data", df)


def update_price(df):
    """
    가격을 다시 수집한 개체들 업데이트
    :param df: 가격을 다시 수집한 개체들 DF
    :return:
    """
    conn = get_connection_to_mycelebs("glamai")
    curs = conn.cursor()
    for idx, row in tqdm(df.iterrows(), total = df.shape[0], desc="error price 수정 "):
        update_qry = "update glamai.sephora_all_data set price = %s, update_date = %s where product_code = %s and item_no = %s;"
        product_code = row['product_code']
        item_no = row['item_no']
        price = float(row['price'])
        now = datetime.now()
        curs.execute(update_qry, [price, now, product_code, item_no])
    conn.commit()
    conn.close()

def get_product_info(cat_id_list = get_category_id()):
    """
    카테고리별 데이터 정보 수집 코드
    :param cat_id_list: sephora 카테고리 아이디 리스트
    :return: 카테고리 하위 개체 정보 반환
    """
    global error_url
    result = []
    counter = 0
    error_url = []

    for idx, row in tqdm(cat_id_list.iterrows(), total=len(cat_id_list), desc="전체 카테고리 하위 개체 수집 "):
        counter += 1
        if counter % 100 == 0:
            time.sleep(random.uniform(60, 100))
        cat_id = row["cat_id"]
        data = get_category_product_data(cat_id=cat_id)
        if type(data) == str:
            error_url.append(data)
        else:
            result.append(data)
        time.sleep(5)

    return result

def get_product_detail_info(result):
    category_cnt = len(result)
    product_result = []

    for category_cnt_num in tqdm(range(category_cnt), desc='상세 정보 수집'):
        product_list_len = len(result[category_cnt_num])

        for product_list_num in range(0,product_list_len):
            try:
                product_cnt = len(result[category_cnt_num][product_list_num]['products'])

                for product_cnt_num in range(0, product_cnt):
                    product_name = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['displayName']
                    brand = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['brandName']
                    url = "https://www.sephora.com" + result[category_cnt_num][product_list_num]['products'][product_cnt_num]["targetUrl"]
                    bigvisual = "https://www.sephora.com" + result[category_cnt_num][product_list_num]['products'][product_cnt_num]['currentSku']['skuImages']['image450']
                    product_code = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['productId']
                    item_no = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['currentSku']['skuId']
                    price = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['currentSku']['listPrice']
                    rating = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['rating']
                    review_count = result[category_cnt_num][product_list_num]['products'][product_cnt_num]['reviews']

                    product_json = {
                        "product_name": product_name,
                        "brand": brand,
                        "url": url,
                        "bigvisual": bigvisual,
                        "product_code": product_code,
                        "item_no": item_no,
                        "price": price,
                        "rating": rating,
                        "review_count": review_count
                    }
                    product_result.append(product_json)
            except:
                pass
    return product_result

# get_product_price 를 위한 함수
def get_price(raw_price):
    if '(' in raw_price:
        price = raw_price[:raw_price.find('(')]
    else:
        price = raw_price.split(' ')[0]

    return price

# get_product_price 를 위한 함수
def get_sku_info(sku_data):
    result = {}
    main_url = 'https://www.sephora.com'
    ## price 받아오기
    price = sku_data['listPrice'][1:]
    result['price'] = get_price(price)
    ## item_no 받아오기
    result['item_no'] = sku_data['skuId']
    result['item_no'] = sku_data.get('skuId', 0)
    ## url 받아오기
    result['url'] = main_url + sku_data.get('targetUrl')
    ## bigvisual 받아오기
    result['bigvisual'] = main_url + sku_data['skuImages']['image450']

    return result


# get_product_price 를 위한 함수
def get_header():
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
    }
    return headers


def get_product_price(df):
    product_list = []

    for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
        now = datetime.now().strftime("%Y-%m-%d %H:%m:%S")
        header_data = get_header()
        product_code = row['product_code']
        url = f'https://www.sephora.com/api/catalog/products/{product_code}?preferedSku=&includeConfigurableSku=true'
        response = requests.get(url, headers=header_data)

        if 'errorCode' in response.text:
            print(product_code, 'error')
            time.sleep(5)
            response = requests.get(url, headers=header_data)
            if 'errorCode' in response.text:
                print(product_code, 'real error')
                pass
            else:
                response_data = response.json()
                current_sku = response_data['currentSku']
                result = get_sku_info(current_sku)
                use_with = None
                result = get_sku_info(current_sku)

                ymal_sku = response_data.get('ymalSkus')
                if ymal_sku:
                    use_with = []
                    for ymal in response_data['ymalSkus']:
                        use_with.append(ymal['productId'])
                    use_with = ','.join(use_with)
                result['product_name'] = response_data.get('displayName')
                result['product_code'] = product_code
                result['brand'] = response_data['brand']['displayName']
                result['regist_date'] = now
                result['update_date'] = now
                result['review_count'] = response_data.get('reviews', 0)
                rating = response_data.get('rating', 0)
                result['rating'] = round(rating * 2) / 2
                product_list.append(result)

                sku_list = response_data.get('regularChildSkus')

        else:
            response_data = response.json()
            current_sku = response_data['currentSku']
            use_with = None
            result = get_sku_info(current_sku)
            ymal_sku = response_data.get('ymalSkus')

            if ymal_sku:
                use_with = []
                for ymal in response_data['ymalSkus']:
                    use_with.append(ymal['productId'])
                use_with = ','.join(use_with)

            result['product_name'] = response_data.get('displayName')
            result['product_code'] = product_code
            result['brand'] = response_data['brand']['displayName']
            result['regist_date'] = now
            result['update_date'] = now
            result['review_count'] = response_data.get('reviews', 0)
            rating = response_data.get('rating', 0)
            result['rating'] = round(rating * 2) / 2
            product_list.append(result)

            sku_list = response_data.get('regularChildSkus')
            if not sku_list:
                sku_list = response_data.get('onSaleChildSkus', [])
            for sku in sku_list:
                result = get_sku_info(sku)
                result['product_name'] = response_data.get('displayName')
                result['product_code'] = product_code
                result['brand'] = response_data['brand']['displayName']
                result['regist_date'] = now
                result['update_date'] = now
                result['review_count'] = response_data.get('reviews', 0)
                rating = response_data.get('rating', 0)
                result['rating'] = round(rating * 2) / 2
                product_list.append(result)
    result = pd.DataFrame(product_list)

    return result

def refine_to_final_data(result_df, category_df):
    result_df['item_no'] = result_df['item_no'].astype("str")
    category_df['item_no'] = category_df['main_sku'].astype("str")

    clean_result_df = result_df.drop_duplicates(['product_code', 'item_no'], keep='first')

    final_data = clean_result_df.merge(category_df, how="left", on=['product_code', "item_no"])
    clean_final_data = final_data[~final_data["main_category"].isnull()]
    clean_final_data = clean_final_data.drop(['main_sku'], axis=1)

    clean_final_data['price_str'] = clean_final_data['price']
    clean_final_data['price'] = clean_final_data['price'].str.replace("$", "")

    pd.set_option('mode.chained_assignment', None)

    price_error_idx = clean_final_data[clean_final_data['price'].str.contains("-")]['price'].index
    for i in price_error_idx:
        clean_final_data['price'][i] = clean_final_data['price'][i].split('-')[0].replace(" ", "")

    clean_final_data['price_str'].astype(str)
    clean_final_data['price'].astype(float)

    now = datetime.now()

    clean_final_data['regist_date'] = now
    clean_final_data['update_date'] = now

    return clean_final_data

def update_all_product():
    error_url = []
    mid_result = []
    counter = 0

    cat_id_list = get_category_id()

    for idx, row in tqdm(cat_id_list.iterrows(), total=len(cat_id_list), desc="전체 카테고리 하위 개체 수집 "):
        counter += 1
        if counter % 100 == 0:
            time.sleep(random.uniform(60, 100))
        cat_id = row["cat_id"]
        data = get_category_product_data(cat_id=cat_id)
        if type(data) == str:
            error_url.append(data)
        else:
            mid_result.append(data)
        time.sleep(5)

    result = get_product_detail_info(mid_result)
    result_df = pd.DataFrame(result)
    category_df = get_category_data()

    clean_final_data = refine_to_final_data(result_df, category_df)
    update_result(clean_final_data)
    
    print("Sephora all product: 업데이트 완료")
    
    return clean_final_data