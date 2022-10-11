import os
import re
import sys
import json
import time
import requests
from datetime import datetime
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

class UpdateProductSale:
    def __init__(self):
        self.__conn__()
        
    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()

    def insert_data_new(self, vertical):
        # insert new product
        query = f'''
                    insert into sephora_{vertical}_data_sale (product_code, item_no, list_price, regist_date)
                    select product_code, item_no, price, regist_date from sephora_{vertical}_data_status
                    where is_use=1 and (product_code, item_no) not in (select product_code, item_no from sephora_{vertical}_data_sale);
                '''
        while True:
            try:
                self.ds_cur.execute(query)
                self.ds_conn.commit()
                print(f'{vertical} new product update 완료!')
                break
            except:
                time.sleep(100)
                self.__close__()
                self.__conn__()
                print("DB 연결 끊김 ... 재연결 성공!")
        
    def get_data(self, vertical):
        table = f'sephora_{vertical}_data_status'
        query = f'select distinct(product_code) from {table} where is_use=1;'
        
        while True:
            try:   
                self.ds_cur.execute(query)
                product_codes = list(self.ds_cur.fetchall())
                break
            except:
                time.sleep(100)
                self.__close__()
                self.__conn__()
                print("DB 연결 끊김 ... 재연결 성공!")
        
        return product_codes

    def scraper_price(self, data, product_code):
        try:
            current_sku = str(data['currentSku'])
            status = 1
        except KeyError:
            status = 0

        if status == 1:
            listPrice_pattern = r"\'listPrice\': \'\$[0-9]{0,5}.[0-9]{0,5}"
            listPrice_re = re.search(listPrice_pattern, current_sku)
            if listPrice_re is None:
                listPrice = float(0)
            else:
                listPrice = listPrice_re.group()
                listPrice = float(listPrice.split(":")[1].replace("'", "").replace(" ", "").replace("$",""))
                
            salePrice_pattern = r"\'salePrice\': \'\$[0-9]{0,5}.[0-9]{0,5}"
            salePrice_re = re.search(salePrice_pattern, current_sku)
            if salePrice_re is None:
                salePrice = float(0)
                is_sale = 0
            else:
                salePrice = salePrice_re.group()
                salePrice = float(salePrice.split(":")[1].replace("'", "").replace(" ", "").replace("$",""))
                is_sale = 1
                
            skuId_pattern = r"\'skuId\': \'[0-9]{5,10}"
            skuId_re = re.search(skuId_pattern, current_sku)
            if skuId_re is None:
                skuId = None
                return None
            else:
                skuId = skuId_re.group()
                item_no = int(skuId.split(":")[1].replace("'", "").replace(" ", ""))
                return [listPrice, salePrice, is_sale, datetime.now(), product_code, item_no]
        
        else:
            return None
        
    def scraper_price_sale(self, data, product_code):
        on_sale_sku = data.get("onSaleChildSkus")
        on_sale_sku_text = str(on_sale_sku)

        listPrice_pattern = r"\'listPrice\': \'\$[0-9]{0,5}.[0-9]{0,5}"
        salePrice_pattern = r"\'salePrice\': \'\$[0-9]{0,5}.[0-9]{0,5}"
        skuId_pattern = r"\'skuId\': \'[0-9]{5,10}"

        listPrice_list = re.findall(listPrice_pattern, on_sale_sku_text)
        salePrice_list = re.findall(salePrice_pattern, on_sale_sku_text)
        skuId_list = re.findall(skuId_pattern, on_sale_sku_text)
        
        if len(skuId_list) == 0:
            return None
        elif len(listPrice_list) == len(salePrice_list) and len(listPrice_list) == len(skuId_list):
            scraped_data = []
            i = 0
            for skuIds in skuId_list:
                list_prices = listPrice_list[i]
                sale_prices = salePrice_list[i]
                
                item_no = int(skuIds.split(":")[1].replace("'", "").replace(" ", ""))
                list_price = float(list_prices.split(":")[1].replace("'", "").replace(" ", "").replace("$",""))
                sale_price = float(sale_prices.split(":")[1].replace("'", "").replace(" ", "").replace("$",""))
                is_sale = 1
                i += 1
                scraped_data.append([list_price, sale_price, is_sale, datetime.now(), product_code, item_no])
            return scraped_data
        else:
            return None
        
    def insert_data(self, data, vertical):
        # update product sales
        query = f'''
                update sephora_{vertical}_data_sale set list_price = %s, sale_price = %s, is_sale = %s, update_date = %s
                where product_code = %s and item_no = %s;
            '''
        while True:
            try:
                self.ds_cur.execute(query, data)
                self.ds_conn.commit()
                break
            except:
                time.sleep(100)
                self.__close__()
                self.__conn__()
                print("DB 연결 끊김 ... 재연결 성공!")
    
    def update_data(self, product_code, vertical):
        product_code = product_code['product_code']
        price_data = []
        url = f'https://www.sephora.com/api/catalog/products/{product_code}?preferedSku=&includeConfigurableSku=true&passkey=caQ0pQXZTqFVYA1yYnnJ9emgUiW59DXA85Kxry8Ma02HE'
        res_data = json_iterator(url)
        status = 0
        if res_data is None:
            status -= 1
        else:
            scraped_data = self.scraper_price(res_data, product_code)
            if scraped_data is None:
                status -= 1
            else:
                self.insert_data(scraped_data, vertical)
                price_data.append(scraped_data)
                status += 1
                
            scraped_datas = self.scraper_price_sale(res_data, product_code)
            if scraped_datas is None:
                status -= 1
            else:
                for scraped_data in scraped_datas:
                    self.insert_data(scraped_data, vertical)
                    price_data.append(scraped_data)
                status += 1
        
        return price_data, status
    
db_glamai = AccessDatabase('glamai')
sale = UpdateProductSale()
def update_sephora_sale(vertical):
    # backup table
    table_name = f'sephora_{vertical}_data_sale'
    db_glamai._backup(table_name=table_name, keep=True)
    
    sale.__conn__()
    product_codes = sale.get_data(vertical)
    sale.insert_data_new(vertical)

    status_info, price_datas = [], []
    for product_code in tqdm(product_codes):
        price_data, status = sale.update_data(product_code, vertical)
        
        status_info.append([product_code, status])
        price_datas += price_data
    sale.__close__()
    print(f'{vertical} product sale status update 완료!')

    return price_datas

def main():
    price_data_dict = {}
    verticals = ['face_base', 'eye', 'lip_color', 'moisturizers', 'cheek', 'treatments', 'masks', 'eye_care', 'body_care', 'mens', 'fragrance_men', 'fragrance_women', 'wellness', 'cleansers']
    for vertical in tqdm(verticals):
        price_data = update_sephora_sale(vertical)
        price_data_dict[vertical] = price_data
        
    return price_data_dict