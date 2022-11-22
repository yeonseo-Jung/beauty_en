import os
import sys
import time
import json
import requests
import datetime
import pymysql
from tqdm import tqdm

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

from crawling.crawler import json_iterator
from database.access import AccessDatabase

tbl_cache = os.path.join(root, 'tbl_cache')
_date = datetime.datetime.today().strftime("%y%m%d")

class ReviewData:
    def __init__(self):
        self.__conn__()

    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()
        
    def get_review_max_date(self):
        sql = '''
            select product_code, max(write_time)
            from sephora_review_date_re group by product_code;
        '''
        self.ds_cur.execute(sql)
        data = self.ds_cur.fetchall()
        return data

    def insert_to_table_review(self, data):
        sql = '''insert into sephora_txt_data_re
        (product_code, color_id, rating, skin_type, eye_color, skin_concerns, hair_color, skin_tone, age, txt_title, txt_data, like_count, write_time, regist_date)
         values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''
        self.ds_cur.executemany(sql, data)
        self.ds_conn.commit()
        
    def _scraper(self, product_code, rev_data):
            title = rev_data.get('Title', '')
            txt_data = rev_data.get('ReviewText')
            if title:
                txt_data = title + ' ' + txt_data

            rating = rev_data.get('Rating', 0)
            context_data = rev_data.get('ContextDataValues', {})
            skin_type_raw = context_data.get('skinType', {})
            skin_type = skin_type_raw.get('Value', '')
            eye_color_raw = context_data.get('eyeColor', {})
            eye_color = eye_color_raw.get('Value', '')
            skin_concerns_raw = context_data.get('skinConcerns', {})
            skin_concerns = skin_concerns_raw.get('Value', '')
            hair_color_raw = context_data.get('hairColor', {})
            hair_color = hair_color_raw.get('Value', '')
            skin_tone_raw = context_data.get('skinTone', {})
            skin_tone = skin_tone_raw.get('Value', '')
            age_raw = context_data.get('age', {})
            age = age_raw.get('Value', '')
            product_id = rev_data.get('ProductId')
            positive_count = rev_data.get('TotalPositiveFeedbackCount')
            return [product_code, product_id, rating, skin_type, eye_color, skin_concerns, hair_color, skin_tone, age, title, txt_data, positive_count]
                    
    def _crawling(self, backup=True):
        
        if backup:
            # table backup
            db_glamai = AccessDatabase('glamai')
            db_glamai._backup(table_name='sephora_txt_data_re', keep=True)
        
        now = datetime.datetime.today()
        product_list = self.get_review_max_date()
        
        txt_data, error = [], []
        for product in tqdm(product_list):
            product_code = product['product_code']
            max_date = product['max(write_time)']
            url = f'https://api.bazaarvoice.com/data/reviews.json?Filter=ProductId%3A{product_code}&Sort=SubmissionTime%3Adesc&Limit=100&Offset=0&Include=Products%2CComments&Stats=Reviews&passkey=rwbw526r2e7spptqd2qzbkp7&apiversion=5.4'
            
            review_data = json_iterator(url)
            if review_data is None:
                note = "url parsing faild"
                error.append([product_code, url, note])
                continue
                    
            review_cnt = review_data.get('TotalResults', 0)
            if not review_cnt:
                # error
                note = "review does not exist"
                error.append([product_code, url, note])
                continue
            end_point = int(review_cnt / 100) + 1
            
            result = []
            for point in range(end_point):
                url = f'https://api.bazaarvoice.com/data/reviews.json?Filter=ProductId%3A{product_code}&Sort=SubmissionTime%3Adesc&Limit=100&Offset={str(100 * point)}&Include=Products%2CComments&Stats=Reviews&passkey=rwbw526r2e7spptqd2qzbkp7&apiversion=5.4'
                
                review_data = json_iterator(url)
                if review_data is None:
                    note = "url pasing faild"
                    error.append([product_code, url, note])
                    continue
                
                flag = False
                reviews = review_data['Results']
                for rev_data in reviews:
                    data = self._scraper(product_code, rev_data)
                    write_time = rev_data.get('SubmissionTime', '')
                    rev_date = write_time.split('.')[0]
                    rev_date = datetime.datetime.strptime(rev_date, '%Y-%m-%dT%H:%M:%S')
                    if rev_date <= max_date:
                        # error
                        note = "reviews that already exist"
                        error.append([product_code, url, note])
                        flag = True
                        break
                    rev_date = rev_date.strftime('%Y-%m-%d %H:%M:%S')
                    time_now = now.strftime('%Y-%m-%d %H:%M:%S')
                    data.append(rev_date)
                    data.append(time_now)
                    result.append(data)
                    
                txt_data += result
                
                if flag:
                    break
            while True:
                try:
                    self.insert_to_table_review(result)
                    break
                except pymysql.err.OperationalError:
                    print('connection 에러, 다시 연결 시작합니다.')
                    self.__close__()
                    self.__conn__()
            
        self.__close__()
        return txt_data, error

class ReviewDate:
    def __init__(self):
        self.__conn__()

    def __conn__(self):
        self.ds = AccessDatabase('glamai')
        self.ds_conn, self.ds_cur = self.ds._connect()
        
    def __close__(self):
        self.ds_conn.commit()
        self.ds_cur.close()
        self.ds_conn.close()
        
    def get_data(self):
        sql = '''
            select product_code, max(write_time) as write_time from sephora_txt_data_re group by product_code;
        '''
        self.ds_cur.execute(sql)
        glamai_data = self.ds_cur.fetchall()
        
        result = []
        for bd in glamai_data:
            recently_write_time = bd['write_time']
            recently_write_time = recently_write_time.strftime('%Y-%m-%d %H:%M:%S')
            result.append((recently_write_time, bd['product_code']))
        
        return result
            
    def update_write_time(self, data):
        update_date = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        sql = f'''update sephora_review_date_re
            set write_time = %s, update_date = \'{update_date}\' where product_code = %s;
        '''
        self.ds_cur.executemany(sql, data)
        self.ds_conn.commit()

    def update_review_date(self):
        result = self.get_data()
        self.update_write_time(result)
        print('Complete Review Date Update')
        
        return result
    
if __name__=='__main__':
    rev = ReviewData()
    txt_data, error = rev._crawling()
    
    time.sleep(100)
    
    revdate = ReviewDate()
    result = revdate.update_review_date()