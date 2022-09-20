import os
import sys
import re
import time
import random
import requests
from datetime import datetime

import html
import pymysql
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

# sephora_{vertical}_data 의 주요 내용을 채우는 클래스, 업데이트 시 항상 sephora_product/product_keyword.py/ProductKeyword() 완료 후 실행
# 메인내용 수집하는 클래스, sephora_product_keyword 테이블 참고
class VerticalData:
    def __init__(self):
        self.__conn__()
        self.vertical_dic = {
            'eye': 'Eye',
            'face_base': 'Face',
            'lip_color': 'Lip',
            'moisturizers': 'Moisturizers',
            'Cheek': 'Cheek',
            'Brush & Applicators': 'Brush & Applicators',
            'Accessories': 'Accessories',
            'Treatments': 'Treatments',
            'Masks': 'Masks',
            'Eye Care': 'Eye Care',
            'Body Care': 'Body Care',
            "Men's Bath & Body": "Men's",
            'Wellness': 'Wellness',
            'Hair Styling': 'Hair Styling',
            'Curly Hair Care': 'Curly Hair Care',
            'Hair Dye & Root Touch-Ups': 'Hair Dye & Root Touch-Ups',
            'Shampoo & Conditioner': 'Shampoo & Conditioner',
            'Hair Treatments': 'Hair Treatments',
            'Bath & Shower': 'Bath & Shower',
            'Cleansers': 'Cleansers',
            'fragrance women': 'fragrance women',
            'fragrance men': 'fragrance men',
            'candles & home scents': 'candles & home scents',
            'sun care (sunscreen)': 'sun care (sunscreen)',
            'self tanner': 'self tanner',
            'Makeup Brushes & Applicators': 'Makeup Brushes & Applicators',
            'Accessories': 'Accessories',
            'Nail polish & Treatments': 'Nail polish & Treatments',
            'Hair tools': 'Hair tools',
        }

    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()

    def get_price(self, raw_price):
        if '(' in raw_price:
            price = raw_price[:raw_price.find('(')]
        else:
            price = raw_price.split(' ')[0]
        return price

    def get_product_list(self, vertical):
        real_vertical = self.vertical_dic[vertical]
        if vertical == "Men's Bath & Body":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "Bath & Body" and mid_category = "Men's"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Hair Styling":
            sql = '''
                select product_code from sephora_product_keyword
                where sub_category = "Hair Styling Products"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Curly Hair Care":
            sql = '''
                select product_code from sephora_product_keyword
                where sub_category = "Curls & Coils"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Hair Dye & Root Touch-Ups":
            sql = '''
                select product_code from sephora_product_keyword
                where sub_category = "Hair Dye & Root Touch-Ups"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Shampoo & Conditioner":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "hair" and mid_category = "Shampoo & Conditioner"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Hair Treatments":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "hair" and mid_category = "Hair Styling & Treatments"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Bath & Shower":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "Bath & Body" and mid_category = "Bath & Shower"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "fragrance women":
            sql = '''
                select product_code from sephora_product_keyword
                where mid_category = "women"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "fragrance men":
            sql = '''
                select product_code from sephora_product_keyword
                where mid_category = "Men"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "candles & home scents":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "Fragrance" and mid_category = "Candles & Home Scents"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "sun care (sunscreen)":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "Skincare" and mid_category = "Sun Care"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "self tanner":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "Skincare" and mid_category = "Self Tanners"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Makeup Brushes & Applicators":
            sql = '''
                select product_code from sephora_product_keyword
                where mid_category = "Brushes & Applicators"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Accessories":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "makeup" and mid_category = "Accessories"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Nail polish & Treatments":
            sql = '''
                select product_code from sephora_product_keyword
                where mid_category = "Nail"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        elif vertical == "Hair tools":
            sql = '''
                select product_code from sephora_product_keyword
                where main_category = "hair" and mid_category = "Hair Tools"
                and is_use = 1;
            '''
            self.ds_cur.execute(sql)
        else:
            sql = '''
                select product_code from sephora_product_keyword
                where mid_category = %s
                and is_use = 1;
            '''
            self.ds_cur.execute(sql, real_vertical)
        product_list = self.ds_cur.fetchall()
        return product_list

    def get_vertical_product(self, vertical):
        if vertical in ['eye', 'face_base', 'lip_color', 'moisturizers']:
            sql = '''
                select item_no from sephora_%s_data
            ''' % vertical
        elif vertical == "Cheek":
            sql = '''
                select item_no from sephora_cheek_data
            '''
        elif vertical == "Treatments":
            sql = '''
                select item_no from sephora_treatments_data
            '''
        elif vertical == "Masks":
            sql = '''
                select item_no from sephora_masks_data
            '''
        elif vertical == "Eye Care":
            sql = '''
                select item_no from sephora_eye_care_data
            '''
        elif vertical == "Body Care":
            sql = '''
                select item_no from sephora_body_care_data
            '''
        elif vertical == "Men's Bath & Body":
            sql = '''
                select item_no from sephora_mens_data
            '''
        elif vertical == "Wellness":
            sql = '''
                select item_no from sephora_wellness_data
            '''
        elif vertical == "Hair Styling":
            sql = '''
                select item_no from sephora_hair_styling_data
            '''
        elif vertical == "Curly Hair Care":
            sql = '''
                select item_no from sephora_curly_hair_care_data
            '''
        elif vertical == "Hair Dye & Root Touch-Ups":
            sql = '''
                select item_no from sephora_hair_dye_root_touchups_data
            '''
        elif vertical == "Shampoo & Conditioner":
            sql = '''
                select item_no from sephora_shampoo_conditioner_data
            '''
        elif vertical == "Hair Treatments":
            sql = '''
                select item_no from sephora_hair_treatments_data
            '''
        elif vertical == "Bath & Shower":
            sql = '''
                select item_no from sephora_bath_shower_data
            '''
        elif vertical == "Cleansers":
            sql = '''
                select item_no from sephora_cleansers_data
            '''
        elif vertical == "fragrance women":
            sql = '''
                select item_no from sephora_fragrance_women_data
            '''
        elif vertical == "fragrance men":
            sql = '''
                select item_no from sephora_fragrance_men_data
            '''
        elif vertical == "candles & home scents":
            sql = '''
                select item_no from sephora_candles_home_scents_data
            '''
        elif vertical == "sun care (sunscreen)":
            sql = '''
                select item_no from sephora_sun_care_data
            '''
        elif vertical == "self tanner":
            sql = '''
                select item_no from sephora_self_tanner_data
            '''
        elif vertical == "Makeup Brushes & Applicators":
            sql = '''
                select item_no from sephora_makeup_brushes_applicators_data
            '''
        elif vertical == "Accessories":
            sql = '''
                select item_no from sephora_accessories_data
            '''
        elif vertical == "Nail polish & Treatments":
            sql = '''
                select item_no from sephora_nail_polish_treatments_data
            '''
        elif vertical == "Hair tools":
            sql = '''
                select item_no from sephora_hair_tools_data
            '''
        else:
            sql = '''
                select item_no from sephora_etc_data
                '''
        self.ds_cur.execute(sql)
        product_list = self.ds_cur.fetchall()
        product_list = [product['item_no'] for product in product_list]
        return product_list

    def get_sku_info(self, sku_data):
        result = {}
        main_url = 'https://www.sephora.com'
        image_list = sku_data.get('alternateImages')
        if image_list:
            result['swatch'] = 'https://www.sephora.com' + image_list[0]['image450']
        else:
            result['swatch'] = None
        color_value = sku_data.get('variationValue')
        color_desc = sku_data.get('variationDesc')
        color = []
        if color_value:
            color.append(color_value)
        if color_desc:
            color.append(color_desc)
        color = ' - '.join(color)

        result['color'] = color
        price = sku_data['listPrice'][1:]
        result['item_no'] = sku_data['skuId']
        result['price'] = self.get_price(price)
        result['size'] = sku_data.get('size')
        result['item_no'] = sku_data.get('skuId', 0)
        result['url'] = main_url + sku_data.get('targetUrl')
        result['max_purchase_quantity'] = sku_data.get('maxPurchaseQuantity', 0)
        result['bigvisual'] = main_url + sku_data['skuImages']['image450']
        result['palette'] = sku_data.get('smallImage')
        result['ingredients'] = sku_data.get('ingredientDesc')
        result['free_shipping'] = sku_data.get('isFreeShippingSku', 0)
        result['gift_wrappable'] = sku_data.get('isGiftWrappable', 0)
        result['limited_edition'] = sku_data.get('isLimitedEdition', 0)
        result['limited_quantity'] = sku_data.get('isLimitedQuantity', 0)
        result['limited_time_offer'] = sku_data.get('isLimitedTimeOffer', 0)
        result['natural_organic'] = sku_data.get('isNaturalOrganic', 0)
        result['online_only'] = sku_data.get('isOnlineOnly', 0)
        result['sephora_exclusive'] = sku_data.get('isSephoraExclusive', 0)
        result['out_of_stock'] = sku_data.get('is_out_of_stock', 0)
        result['max_purchase_quantity'] = sku_data.get('maxPurchaseQuantity', 0)
        result['paypal_restrict'] = sku_data.get('isPaypalRestricted', 0)

        return result

    # def get_product_info(self, product_code): # 100개 정도
    #     url = f'https://www.sephora.com/api/catalog/products/{product_code}?preferedSku=&includeConfigurableSku=true'
    #     # response = requests.get(url, headers=self.get_headers())
    #     # if 'errorCode' in response.text:
    #     #     print(product_code, 'error')
    #     #     time.sleep(random.randint(5,10))
    #     #     response = requests.get(url, headers=self.get_headers())
    #     #     if 'errorCode' in response.text:
    #     #         print(product_code, 'repeated error')
    #     #         return None
    #     product_list = []
    #     global ymal_sku
    #     global result
    #     global response_data
        
    #     response_data = json_iterator(url)
    #     if response_data is None:
    #         return None
    #     else:
    #         pass
        
    #     try:
    #         # response_data = response.json()
    #         current_sku = response_data.get('currentSku')
    #         result = self.get_sku_info(current_sku)
    #         ymal_sku = response_data.get('ymalSkus')
    #         use_with = ''
    #     except Exception as e:
    #         ymal_sku = False
    #         print(e)
    #         print('pass')
    #     if ymal_sku:
    #         use_with = []
    #         for ymal in response_data.get('ymalSkus'):
    #             use_with.append(ymal['productId'])
    #         use_with = ','.join(use_with)

    #     try:
    #         result['brand'] = response_data.get('brand')['displayName']
    #     except KeyError as ke:
    #         print(ke)
    #         pass

    #     result['product_code'] = product_code
    #     result['is_sale'] = response_data.get('onSaleSku', 0)
    #     result['main_sku_check'] = 1

    #     try:
    #         result['use_with'] = use_with
    #     except:
    #         pass
    #     result['like_count'] = response_data.get('lovesCount', 0)
    #     result['review_count'] = response_data.get('reviews', 0)
    #     result['what_it_is'] = response_data.get('quickLookDescription')
    #     rating = response_data.get('rating', 0)
    #     result['rating'] = round(rating * 2) / 2
    #     result['product_name'] = response_data.get('displayName')
    #     ## details 부분 추가
    #     result['details'] = response_data.get('longDescription')
    #     if response_data.get('longDescription') == None:
    #         result['details'] = ""
    #     else:
    #         result['details'] = html.unescape(result['details']).replace("<b>", "").replace("</b>", "").replace("<br>",
    #                                                                                                             "").replace(
    #             "</br>", "").replace("\r", "").replace("\n", "").replace("\t", "")
    #         result['details'] = re.sub('<.+?>', '', result['details'])
    #     product_list.append(result)
    #     sku_list = response_data.get('regularChildSkus')
    #     if not sku_list:
    #         sku_list = response_data.get('onSaleChildSkus', [])

    #     for sku in sku_list:
    #         result = self.get_sku_info(sku)
    #         result['product_code'] = product_code
    #         try:
    #             result['brand'] = response_data.get('brand')['displayName']
    #         except KeyError as ke:
    #             print(ke)
    #             pass
    #         result['is_sale'] = response_data.get('onSaleSku', 0)
    #         result['main_sku_check'] = 0
    #         try:
    #             result['use_with'] = use_with
    #         except:
    #             pass
    #         result['like_count'] = response_data.get('lovesCount', 0)
    #         result['review_count'] = response_data.get('reviews', 0)
    #         result['what_it_is'] = response_data.get('quickLookDescription')
    #         rating = response_data.get('rating', 0)
    #         result['rating'] = round(rating * 2) / 2
    #         result['product_name'] = response_data.get('displayName')
    #         ## details 부분 추가
    #         result['details'] = response_data.get('longDescription')
    #         if response_data.get('longDescription') == None:
    #             result['details'] == ""
    #         else:
    #             result['details'] = html.unescape(result['details']).replace("<b>", "").replace("</b>", "").replace(
    #                 "<br>", "").replace("</br>", "").replace("\r", "").replace("\n", "").replace("\t", "")
    #             result['details'] = re.sub('<.+?>', '', result['details'])
    #         product_list.append(result)

    #     return product_list
    
    def get_product_info(self, product_code): # 100개 정도
        url = f'https://www.sephora.com/api/catalog/products/{product_code}?preferedSku=&includeConfigurableSku=true'
        # response = requests.get(url, headers=self.get_headers())
        # if 'errorCode' in response.text:
        #     print(product_code, 'error')
        #     time.sleep(random.randint(5,10))
        #     response = requests.get(url, headers=self.get_headers())
        #     if 'errorCode' in response.text:
        #         print(product_code, 'repeated error')
        #         return None
        product_list = []
        global ymal_sku
        global result
        global response_data
        
        response_data = json_iterator(url)
        if response_data is None:
            print(product_code, 'is None')
            return None
        else:
            status = 1
            current_sku = response_data.get('currentSku')
            if current_sku is None:
                status = 0
            else:
                try:
                    result = self.get_sku_info(current_sku)
                except Exception as e:
                    status = 0
                    print(e, product_code)
        
        if status == 1:
            ymal_sku = response_data.get('ymalSkus')
            if ymal_sku is None:
                use_with = ''
            else:
                use_with = []
                for ymal in ymal_sku:
                    use_with.append(ymal['productId'])
                use_with = ','.join(use_with)
            result['use_with'] = use_with
            
            # brand
            try:
                result['brand'] = response_data.get('brand')['displayName']
            except Exception as e:
                result['brand'] = ''
                print(e, product_code, '** brand is None **')

            result['product_code'] = product_code
            result['is_sale'] = response_data.get('onSaleSku', 0)
            result['main_sku_check'] = 1
            result['like_count'] = response_data.get('lovesCount', 0)
            result['review_count'] = response_data.get('reviews', 0)
            result['what_it_is'] = response_data.get('quickLookDescription')
            rating = response_data.get('rating', 0)
            result['rating'] = round(rating * 2) / 2
            result['product_name'] = response_data.get('displayName')
            
            # details 부분 추가
            result['details'] = response_data.get('longDescription')
            if response_data.get('longDescription') is None:
                result['details'] = ""
            else:
                result['details'] = html.unescape(result['details']).replace("<b>", "").replace("</b>", "").replace("<br>", "").replace("</br>", "").replace("\r", "").replace("\n", "").replace("\t", "")
                result['details'] = re.sub('<.+?>', '', result['details'])
                
            product_list.append(result)
        
        # regularChildSkus
        sku_list = response_data.get('regularChildSkus')
        if sku_list is None:
            sku_list = response_data.get('onSaleChildSkus', [])

        for sku in sku_list:
            status = 1
            try:
                result = self.get_sku_info(sku)
            except Exception as e:
                status = 0
                print(e, product_code)
            if status == 1:
                result['product_code'] = product_code
                # brand
                try:
                    result['brand'] = response_data.get('brand')['displayName']
                except Exception as e:
                    result['brand'] = ''
                    print(e, product_code, '** brand is None **')
                result['is_sale'] = response_data.get('onSaleSku', 0)
                result['main_sku_check'] = 0
                try:
                    result['use_with'] = use_with
                except:
                    pass
                result['like_count'] = response_data.get('lovesCount', 0)
                result['review_count'] = response_data.get('reviews', 0)
                result['what_it_is'] = response_data.get('quickLookDescription')
                rating = response_data.get('rating', 0)
                result['rating'] = round(rating * 2) / 2
                result['product_name'] = response_data.get('displayName')
                
                # details 부분 추가
                result['details'] = response_data.get('longDescription')
                if response_data.get('longDescription') is None:
                    result['details'] = ""
                else:
                    result['details'] = html.unescape(result['details']).replace("<b>", "").replace("</b>", "").replace("<br>", "").replace("</br>", "").replace("\r", "").replace("\n", "").replace("\t", "")
                    result['details'] = re.sub('<.+?>', '', result['details'])
                product_list.append(result)

        return product_list

    def get_sql_by_work(self, work, vertical):
        if work == 'insert':
            if vertical in ['eye', 'face_base', 'lip_color', 'moisturizers']:
                sql = '''
                        insert into 
                        sephora_%s_data(%%s, regist_date, update_date)
                        values(%%s, current_timestamp, now())
                        ''' % vertical
            elif vertical == "Cheek":
                sql = '''
                       insert into 
                       sephora_cheek_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            ## 210615 수정 : 추가된 개체
            elif vertical == "Treatments":
                sql = '''
                       insert into 
                       sephora_treatments_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Masks":
                sql = '''
                       insert into 
                       sephora_masks_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Eye Care":
                sql = '''
                       insert into 
                       sephora_eye_care_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Body Care":
                sql = '''
                       insert into 
                       sephora_body_care_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Men's Bath & Body":
                sql = '''
                       insert into 
                       sephora_mens_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Wellness":
                sql = '''
                       insert into 
                       sephora_wellness_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            ## 210706 추가
            elif vertical == "Hair Styling":
                sql = '''
                       insert into 
                       sephora_hair_styling_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Curly Hair Care":
                sql = '''
                       insert into 
                       sephora_curly_hair_care_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Hair Dye & Root Touch-Ups":
                sql = '''
                       insert into 
                       sephora_hair_dye_root_touchups_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Shampoo & Conditioner":
                sql = '''
                       insert into 
                       sephora_shampoo_conditioner_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Hair Treatments":
                sql = '''
                       insert into 
                       sephora_hair_treatments_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Bath & Shower":
                sql = '''
                       insert into 
                       sephora_bath_shower_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Cleansers":
                sql = '''
                       insert into 
                       sephora_cleansers_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "fragrance women":
                sql = '''
                       insert into 
                       sephora_fragrance_women_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "fragrance men":
                sql = '''
                       insert into 
                       sephora_fragrance_men_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "candles & home scents":
                sql = '''
                       insert into 
                       sephora_candles_home_scents_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "sun care (sunscreen)":
                sql = '''
                       insert into 
                       sephora_sun_care_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "self tanner":
                sql = '''
                       insert into 
                       sephora_self_tanner_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Makeup Brushes & Applicators":
                sql = '''
                       insert into 
                       sephora_makeup_brushes_applicators_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Accessories":
                sql = '''
                       insert into 
                       sephora_accessories_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Nail polish & Treatments":
                sql = '''
                       insert into 
                       sephora_nail_polish_treatments_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            elif vertical == "Hair tools":
                sql = '''
                       insert into 
                       sephora_hair_tools_data(%s, regist_date, update_date)
                       values(%s, current_timestamp, now())
                        '''
            else:
                sql = '''
                        insert into 
                        sephora_etc_data(%s, regist_date, update_date)
                        values(%s, current_timestamp, now())
                        '''
        else:
            if vertical in ['eye', 'face_base', 'lip_color', 'moisturizers']:
                sql = '''
                        update sephora_%s_data
                        set %%s, update_date = now()
                        ''' % vertical
            elif vertical == "Cheek":
                sql = '''
                        update sephora_cheek_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Treatments":
                sql = '''
                        update sephora_treatments_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Masks":
                sql = '''
                        update sephora_masks_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Eye Care":
                sql = '''
                        update sephora_eye_care_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Body Care":
                sql = '''
                        update sephora_body_care_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Men's Bath & Body":
                sql = '''
                        update sephora_mens_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Wellness":
                sql = '''
                        update sephora_wellness_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Hair Styling":
                sql = '''
                        update sephora_hair_styling_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Curly Hair Care":
                sql = '''
                        update sephora_curly_hair_care_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Hair Dye & Root Touch-Ups":
                sql = '''
                        update sephora_hair_dye_root_touchups_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Shampoo & Conditioner":
                sql = '''
                        update sephora_shampoo_conditioner_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Hair Treatments":
                sql = '''
                        update sephora_hair_treatments_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Bath & Shower":
                sql = '''
                        update sephora_bath_shower_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Cleansers":
                sql = '''
                        update sephora_cleansers_data
                        set %s, update_date = now()
                        '''
            elif vertical == "fragrance women":
                sql = '''
                        update sephora_fragrance_women_data
                        set %s, update_date = now()
                        '''
            elif vertical == "fragrance men":
                sql = '''
                        update sephora_fragrance_men_data
                        set %s, update_date = now()
                        '''
            elif vertical == "candles & home scents":
                sql = '''
                        update sephora_candles_home_scents_data
                        set %s, update_date = now()
                        '''
            elif vertical == "sun care (sunscreen)":
                sql = '''
                        update sephora_sun_care_data
                        set %s, update_date = now()
                        '''
            elif vertical == "self tanner":
                sql = '''
                        update sephora_self_tanner_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Makeup Brushes & Applicators":
                sql = '''
                        update sephora_makeup_brushes_applicators_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Accessories":
                sql = '''
                        update sephora_accessories_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Nail polish & Treatments":
                sql = '''
                        update sephora_nail_polish_treatments_data
                        set %s, update_date = now()
                        '''
            elif vertical == "Hair tools":
                sql = '''
                        update sephora_hair_tools_data
                        set %s, update_date = now()
                        '''
            else:
                sql = '''
                        update sephora_etc_data
                        set %s, update_date = now()
                    '''
        return sql

    def get_columns(self, vertical):
        if vertical in ['Eye', 'Face', 'Lip', 'Moisturizers']:
            sql = 'show columns from sephora_%s_data' % vertical
        else:
            sql = 'show columns from sephora_etc_data'
        try:
            self.ds_cur.execute(sql)
        except pymysql.err.OperationalError:
            print('연결끊김, 다시 연결 합니다.')
            time.sleep(100)
            self.__close__()
            self.__conn__()
            self.ds_cur.execute(sql)
        data = self.ds_cur.fetchall()
        keys = [dt['Field'] for dt in data]
        keys.remove('pk')
        keys.remove('regist_date')
        keys.remove('update_date')
        try:
            del keys[keys.index('rgb')]
        except:
            pass
        return keys

    def update_vertical_data(self):
        # verticals = [
        #     'eye', 'face_base', 'lip_color', 'moisturizers', 'Cheek', 'Brush & Applicators', 'Accessories',
        #     'Treatments', 'Masks', 'Eye Care', 'Body Care', "Men's Bath & Body", 'Wellness',
        #     'Hair Styling', 'Curly Hair Care', 'Hair Dye & Root Touch-Ups', 'Shampoo & Conditioner',
        #     'Hair Treatments', 'Bath & Shower', 'Cleansers', 'fragrance women', 'fragrance men',
        #     'candles & home scents', 'sun care (sunscreen)', 'self tanner', 'Makeup Brushes & Applicators',
        #     'Accessories', 'Nail polish & Treatments', 'Hair tools'
        # ]
        
        verticals = [
            'Curly Hair Care', 'Hair Dye & Root Touch-Ups', 'Shampoo & Conditioner',
            'Hair Treatments', 'Bath & Shower', 'Cleansers', 'fragrance women', 'fragrance men',
            'candles & home scents', 'sun care (sunscreen)', 'self tanner', 'Makeup Brushes & Applicators',
            'Accessories', 'Nail polish & Treatments', 'Hair tools'
        ]

        for vertical in tqdm(verticals):
            print('\n\n========================================')
            print(f'vertical: {vertical}')
            data_columns = self.get_columns(vertical)
            insert_data = []
            update_data = []
            remain_product = self.get_vertical_product(vertical)
            products = self.get_product_list(vertical)
            counter = 0
            for product in tqdm(products):
                # counter += 1
                # if counter % 50 == 0:
                #     time.sleep(random.uniform(5, 10))
                # if counter % 500 == 0:
                #     time.sleep(random.uniform(40, 60))
                crawl_product_list = self.get_product_info(product['product_code'])
                if crawl_product_list:
                   try:
                    for crawl_product in crawl_product_list:
                        item_no = int(crawl_product['item_no'])
                        data_list = [crawl_product[columns] for columns in data_columns]
                        if item_no in remain_product:
                            data_list.append(item_no)
                            update_data.append(data_list)
                        else:
                            insert_data.append(data_list)
                   except KeyError as ke:
                       print(ke)
                       pass
                else:
                    continue

            if update_data or insert_data:
                insert_sql = self.get_sql_by_work('insert', vertical)
                update_sql = self.get_sql_by_work('update', vertical)
                insert_sql = insert_sql % (', '.join(data_columns), ', '.join(['%s' for _ in range(len(data_columns))]))
                update_sql = update_sql % (', '.join([dc + ' = %s' for dc in data_columns]))
                flag = True
                while flag:
                    try:
                        self.ds_cur.executemany(insert_sql, insert_data)
                        self.ds_conn.commit()
                        flag = False
                    except pymysql.err.OperationalError:
                        print('Connection Error, 다시 연결 후 실행합니다.')
                        time.sleep(100)
                        self.__close__()
                        self.__conn__()

                update_sql += 'where item_no = %s'
                update_chunk = [update_data[x: x + 200] for x in range(0, len(update_data), 50)]
                print('length of update_data is : ', len(update_chunk))
                print('========================================\n\n')

                for chunk in tqdm(update_chunk):
                    try:
                        self.ds_cur.executemany(update_sql, chunk)
                    except pymysql.err.OperationalError:
                        print('Connection Error, 다시 연결 후 실행합니다.')
                        time.sleep(100)
                        self.__close__()
                        self.__conn__()
                        self.ds_cur.executemany(update_sql, chunk)
                    self.ds_conn.commit()
            else:
                continue
            
        self.__close__()
        print("sephora_vertical_data 업데이트 완료")
