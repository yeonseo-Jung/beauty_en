import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
import pandas as pd
import numpy as np
import datetime
import json
from user_agent import generate_user_agent
import sys
import os
from .logger import main

from database.access import AccessDatabase
db_glamai = AccessDatabase('glamai')
DIR_NAME = os.path.dirname(__file__)
os.chdir(DIR_NAME)


'''
# 글램아이 코드리뷰
1. 쿼리문 가져오기 glamai db내에서 오늘기준 최근 일주일기간의 regist_date에 해당하는 sephora_eye_data, sephora_face_base_data, sephora_lip_color_data, sephora_moisturizers_data 테이블의 일부 중복값 없는 컬럼데이터 가져온다.<br>
3. 쿼리문으로 가져온 데이터를 iterate하면서 df.product_name, df.brand기준(title로 변수저장한다) <br>
4. title을 search_word변수에 담는다.<br>
5. 각각의 search_word변수 기준으로 구글 및 유튜브 키워드를 수집한다 (soup과 request 활용한다. selenium 해당없음)<br>
6. 전처리 (콤마 및 쓰레기문자 제거) 과정을 거친다.<br>
7. xlsx, csv export한다.
'''



class GoogleWordCrawler:
    def __init__(self, search_word):
        self.search_word = search_word
        self.session = requests.Session()
        url = 'https://www.google.com/search?&q=' + self.search_word
        self.ua = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
        headers_data = {'user-agent': self.ua}
        res = self.session.get(url, headers=headers_data)
        
        self.soup = BeautifulSoup(res.text, 'lxml')
        self.google_crawled_data = []
        
    
    def crawl_related_words(self):
        related_word_areas = self.soup.find_all('p', class_='nVcaUb')
        for related_word_area in related_word_areas:
            related_word = related_word_area.a.text
            related_word_link = related_word_area.a['href']
            data = {'site': 'google', 'crk_type': 'related',
                    'query': self.search_word, 'keyword': related_word}
            
            self.google_crawled_data.append(data)
            
    
    def crawl_auto_complete_words(self):
        
        cookie_dict = self.session.cookies.get_dict()
        key_list = cookie_dict.keys()
        cookie_string = ''
        for key in key_list:
            cookie_string += str(key) + "=" + str(cookie_dict[f'{key}']) + ";"
        
        url = 'https://www.google.com/complete/search?&q=' + str(self.search_word) + "&client=psy-ab"
        headers_data = {
            'user-agent': self.ua,
            'cookie': cookie_string
        }
        response_data = self.session.get(url, headers=headers_data)
        json_file = json.loads(response_data.text)
        temp_auto_complete_words_list = str(json_file).replace("[", '').replace("]", '').replace("<b>","").replace("</b>","").replace("&#39;","").replace(',0', '').replace("\"", "").split(',')
        
        
        
        auto_complete_words_list = []
        
        for i in range(len(temp_auto_complete_words_list)):
            if i == 0:
                continue
            else:
                no_use_detect = re.compile(r"[:,{,]|^ +[0-9]+$")
                if no_use_detect.search(temp_auto_complete_words_list[i]):
                    pass
                else:
                    auto_complete_words_list.append(
                        temp_auto_complete_words_list[i])
        for word in auto_complete_words_list:
            data = {'site': 'google', 'crk_type': 'recommend',
                    'query': self.search_word, 'keyword': word.replace("\'","")}
            
            self.google_crawled_data.append(data)
            
    def crawl(self):
        self.crawl_auto_complete_words()
        self.crawl_related_words()
        return self.google_crawled_data
    

class YoutubeWordCrawler:
    def __init__(self, search_word):
        self.search_word = search_word
        self.session = requests.Session()
        url = 'https://clients1.google.com/complete/search?client=youtube&hl=en&q=' + self.search_word
        self.ua = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
        headers_data = {'user-agent': self.ua }
        res = self.session.get(url, headers=headers_data)
        self.response = res.text
        self.youtube_crawled_data = []
        
    def get_recommend_words(self):
        temp_list = self.response.replace("[", "").replace("]", "").replace(
            ",0", "").replace('\"', '').replace('window.google.ac.h', '').split(',')
        recommend_list = []
        
        for i in range(len(temp_list)):
            if i == 0:
                continue
            else:
                no_use_detect = re.compile(r"[:,{,]|^[0-9]+$")
                if no_use_detect.search(temp_list[i]):
                    pass
                else:
                    recommend_list.append(temp_list[i])
        for word in recommend_list:
            data = {'site': 'youtube', 'crk_type': 'recommend',
                    'query': self.search_word, 'keyword': word.replace("\'","")}
            
            self.youtube_crawled_data.append(data)
        return self.youtube_crawled_data
    
    
class WordCloudAssembler:
    def __init__(self, tmdb_id, title):
        self.tmdb_id = tmdb_id
        self.title = title
        dt = datetime.datetime.now()
        year = dt.strftime("%Y")
        month = int(dt.strftime("%m"))-1
        month = str(month).zfill(2)
        
        self.monthly_label = year+"_"+month
        self.search_word_list = [""]
        
    def assemble_word_cloud_data(self):
        total_list = []
        
        for word in self.search_word_list:
            try: 
                search_word = self.title + " " + word
                search_word = search_word.strip()
                google = GoogleWordCrawler(search_word)
                youtube = YoutubeWordCrawler(search_word)
                google_result = google.crawl()
                youtube_result = youtube.get_recommend_words()
                total_list = total_list + google_result + youtube_result
            except Exception as e:
                print(e)
        return  total_list


def f7(seq):
    
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def DeleteDupedKeywords(row):
    
    
    not_duped_keywords_list = f7(row['keywords'].split(','))
    one_string_keywords = ','.join(not_duped_keywords_list)
    return one_string_keywords


def GlamaiCleanKeyword(keywords):
    keywords = re.sub(r'eye liner', r'eyeliner', keywords)
    keywords = re.sub(r'eye shadow', r'eyeshadow', keywords)
    keywords = re.sub(r'eye lash', r'eyelash', keywords)
    keywords = re.sub(r'eye lashes', r'eyelashes', keywords)
    keywords = re.sub(r'eye brow', r'eyebrow', keywords)
    
    keywords = re.sub(r'lipliner', r'lip liner', keywords)
    keywords = re.sub(r'lipplumper', r'lip plumper', keywords)
    keywords = re.sub(r'lipset', r'lip set', keywords)
    keywords = re.sub(r'lip stick', r'lipstick', keywords)
    keywords = re.sub(r'lipbalm', r'lip balm', keywords)
    keywords = re.sub(r'lipstain', r'lip stain', keywords)
    keywords = re.sub(r'lipgloss', r'lip gloss', keywords)
    return keywords


def CleanText(total_df:object, GlamaiMode=True):
    
    total_df['keywords'] = [x.replace(',,',',') for x in total_df['keywords']]
    total_df['keywords'] = [x.replace(' , ',',') for x in total_df['keywords']]
    total_df['keywords'] = [x.replace(', ',',') for x in total_df['keywords']]
    total_df['keywords'] = [x.replace(' ,',',') for x in total_df['keywords']]
    total_df['keywords'] = [re.sub('^,','',x).rstrip() for x in total_df['keywords']]
    total_df['keywords'] = [x.lstrip() for x in total_df['keywords']]
    total_df['keywords'] = [str(x).split(',') for x in total_df['keywords']]
    total_df['keywords'] = [list(filter(None, x)) for x in total_df['keywords']]
    total_df['keywords'] = [','.join(x) for x in total_df['keywords']]
    total_df['keywords'] = [x.replace('nan','') for x in total_df['keywords']]

    total_df['keywords'] = total_df['keywords'].apply(lambda x: x.encode().decode('unicode_escape', 'ignore'))
    total_df['keywords'] = [x.encode('latin-1','ignore').decode('utf-8', 'ignore') for x in total_df['keywords']]

    total_df['keywords'] = total_df['keywords'].str.replace('&amp;', '&', regex=False)
    
    
    total_df['keywords'] = total_df['keywords'].replace('', np.nan, regex=False)
    
    
    total_df = total_df[total_df['keywords'].notnull()]
    
    
    total_df = total_df.drop_duplicates(subset=['product_code', 'keywords'], keep="first") 
    
    
    total_df['keywords'] = total_df.apply(lambda x : DeleteDupedKeywords(x), axis=1)
    
    
    
    total_df['keywords'] = total_df['keywords'].str.replace(pat=r'[^a-zA-Z,]+', repl=r' ', regex=True)
    
    
    total_df['keywords'] = total_df['keywords'].str.replace(pat=r'  +', repl=r' ', regex=True)
    
    
    total_df['keywords'] = total_df['keywords'].str.replace(pat=r' ,', repl=r',', regex=True)
    total_df['keywords'] = total_df['keywords'].str.replace(pat=r', ', repl=r',', regex=True)
    
    
    total_df['keywords'] = total_df['keywords'].str.strip()
    
    
    total_df['keywords'] = total_df.apply(lambda x: GlamaiCleanKeyword(x['keywords']), axis=1)
    
    
    total_df = total_df.drop_duplicates(subset=['product_code', 'keywords'], keep="first") 
    
    
    total_df.reset_index(inplace=True)
    del total_df['index']
    
    return total_df


def update_search_keywords():

    log_df = pd.read_csv('Glamai작업로그파일.csv')

    global total_df
    
    last_crawl_date = log_df.iloc[-1]['마지막 작업일자']
    print('마지막 작업일자:', last_crawl_date)
    
    query = f'''
    SELECT DISTINCT product_code, product_name, brand, regist_date
    FROM glamai.sephora_eye_data
    WHERE regist_date >= "{last_crawl_date}"
    UNION
    SELECT DISTINCT product_code, product_name, brand, regist_date
    FROM glamai.sephora_face_base_data
    WHERE regist_date >= "{last_crawl_date}"
    UNION
    SELECT DISTINCT product_code, product_name, brand, regist_date
    FROM glamai.sephora_lip_color_data
    WHERE regist_date >= "{last_crawl_date}"
    union
    SELECT DISTINCT product_code, product_name, brand, regist_date
    FROM glamai.sephora_moisturizers_data 
    WHERE regist_date >= "{last_crawl_date}"
    '''
    conn, curs = db_glamai._connect()
    df = pd.read_sql(query, con=conn)

    if df.empty:
        raise Exception('Empty DataFrame')

    total_data = []
    today = datetime.datetime.now()
    date = today.strftime("%Y%m%d")
    
    first_index = 0
    last_index = len(df)
    
    for i in tqdm(range(first_index, last_index)):
        try:
            product_code = df["product_code"][i]
            title = '%s %s' % (df["product_name"][i], df["brand"][i])
            wordcloud = WordCloudAssembler(product_code,title)
            result = wordcloud.assemble_word_cloud_data()
            if len(result) == 0:
                continue
                
            result_df = pd.DataFrame(result)
            
            if result_df["keyword"].any():
                result_df = result_df.groupby(result_df["keyword"]).size()
                result_count = pd.DataFrame(result_df)
                result_count = result_count.rename(columns = {0:'result_count'})
                result_count =  result_count.sort_values(["result_count"],ascending=False)
                
                keywords = ""
                korean_detector = re.compile("[가-힣]")
                for j in range(len(result_count)):
                    if korean_detector.search(str(result_count.index[j])):
                        pass
                    else:
                        if keywords == '':
                            keywords = str(result_count.index[j])
                        else:
                            keywords= keywords +","+ str(result_count.index[j])
                data = {"product_code":product_code,"keywords":keywords,"word_length":len(result_count)}
                
                total_data.append(data)
            else:
                pass
            
        except Exception as err:
            print(err)
            total_df = pd.DataFrame(total_data)
            
            
            total_df = CleanText(total_df)
            
            
            total_df.to_excel(f"glamai_wordcloud_{date}_{first_index}_to_{str(i-1)}.xlsx")
            sys.exit()
    
    
    total_df = pd.DataFrame(total_data)
    
    total_df = CleanText(total_df)

    total_df['regist_date'] = today
    total_df['update_date'] = today
    
    last_index = df.shape[0] 

    upload_table_name = 'glamai_search_keywords'
    db_glamai.engine_upload(upload_df=total_df, table_name=upload_table_name, if_exists_option="append")
    return total_df

def db_distinction():
    conn, curs = db_glamai._connect()
    query = '''
            DELETE
            FROM glamai.glamai_search_keywords
            WHERE gsk_pk not in (
                                SELECT T.gsk_pk
                                FROM (
                                SELECT product_code, keywords, count(*) AS cnt, MAX(gsk_pk) AS gsk_pk
                                FROM glamai.glamai_search_keywords
                                GROUP BY product_code, keywords
                                ) T);
                                '''
    
    affected_rows = curs.execute(query)
    print('affected rows:', affected_rows)
    conn.commit()
    curs.close()
    conn.close()