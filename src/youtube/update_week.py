import os
import sys
import selenium 
import pandas as pd
import time
import pymysql
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from tqdm import tqdm
from datetime import datetime, timedelta

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)
    
from database.conn import AccessDatabase

db_glamai = AccessDatabase("glamai")

def get_input_data():
    # 신규 수집 데이터
    # before_a_week = (datetime.now() + timedelta(-10)).strftime('%y%m%d')
    query = """
        SELECT regist_date 
        FROM glamai_youtube_urls
        ORDER BY regist_date DESC
        LIMIT 1;
    """
    data = db_glamai._execute(query)
    before_a_week = data[0]["regist_date"].strftime("%Y-%m-%d")
    conn, curs = db_glamai._connect()
    data = pd.read_sql(f'''
    select distinct product_code, product_name, brand from glamai.sephora_eye_data where regist_date > '{before_a_week}'
    union
    select distinct product_code, product_name, brand from glamai.sephora_face_base_data where regist_date > '{before_a_week}'
    union
    select distinct product_code, product_name, brand from glamai.sephora_lip_color_data where regist_date > '{before_a_week}'
    union
    select distinct product_code, product_name, brand from glamai.sephora_moisturizers_data where regist_date > '{before_a_week}';''', 
    con=conn
    )
    return data

def get_driver():
    # get webdriver 
    options = webdriver.ChromeOptions()
    options.add_argument('--incognito')
    prefs = {
        'profile.default_content_setting_values': {
            'cookies': 2,
            'images': 1,
            'plugins': 2,
            'popups': 2,
            'geolocation': 2,
            'notifications': 2,
            'auto_select_certificate': 2,
            'fullscreen': 2,
            'mouselock': 2,
            'mixed_script': 2,
            'media_stream': 2,
            'media_stream_mic': 2,
            'media_stream_camera': 2,
            'protocol_handlers': 2,
            'ppapi_broker': 2,
            'automatic_downloads': 2,
            'midi_sysex': 2,
            'push_messaging': 2,
            'ssl_cert_decisions': 2,
            'metro_switch_to_desktop': 2,
            'protected_media_identifier': 2,
            'app_banner': 2,
            'site_engagement': 2,
            'durable_storage': 2
        }
    }
    options.add_experimental_option('prefs', prefs)
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    return driver

def crawling_youtube():
    input_data = get_input_data()
    drv = get_driver()
    data_list, na_list = [], []
    for idx, row in tqdm(input_data.iterrows(), total=input_data.shape[0], desc="ing"):

        product_code = row['product_code']
        search_query = (row['product_name']).replace("&", " ") + " " + row['brand']

        drv.get('https://www.youtube.com/results?search_query=' +
                str(search_query))
        time.sleep(3)

        no_of_pagedowns = 10
        elem = drv.find_element(By.TAG_NAME, "body")

        while no_of_pagedowns:
            elem.send_keys(Keys.PAGE_DOWN)
            time.sleep(1.2)
            no_of_pagedowns -= 1

        soup = BeautifulSoup(drv.page_source, "lxml")
        time.sleep(2)  

        contents = soup.findAll("ytd-video-renderer",
                                class_='style-scope ytd-item-section-renderer')

        if len(contents) == 0:
            na = {'product_code': product_code, 'search_query': search_query}
            na_list.append(na)
        else:
            no = 0
            for i in contents:
                try:
                    title = i.find('a', id='video-title').get('title')
                except:
                    title = None

                try:
                    yt_url = i.find('a', id='video-title').get('href')
                except:
                    yt_url = None

                try:
                    video_id = yt_url.replace('/watch?v=','').replace('/shorts/', '')
                    thumbnail = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
                except:
                    thumbnail = None

                try:
                    youtuber = i.find(
                        'a',
                        class_='yt-simple-endpoint style-scope yt-formatted-string'
                    ).text
                except:
                    youtuber = None

                try:
                    duration = i.find(
                        'span',
                        class_=
                        'style-scope ytd-thumbnail-overlay-time-status-renderer'
                    ).text.strip()
                except:
                    duration = None

                dataii = {
                    "product_code": product_code,
                    "thumbnail": thumbnail,
                    "title": title,
                    "yt_url": yt_url,
                    "duration": duration,
                    "youtuber": youtuber
                }
                data_list.append(dataii)
                no += 1
                if no == 20:
                    break
    drv.close()
    return data_list, na_list

def preprocessor():
    data_list, na_list = crawling_youtube()

    result_df = pd.DataFrame(
        data_list,
        columns=[
        'product_code', 'thumbnail', 'title', 'yt_url', 'duration', 'youtuber'
        ]
    )
    result_df = result_df[result_df['thumbnail'].notnull()]
    result_df = result_df[result_df['title'].notnull()]
    result_df = result_df[result_df['yt_url'].notnull()]

    # thumbnail
    result_df['thumbnail'] = [x.split('?') for x in result_df['thumbnail']]
    result_df['thumbnail'] = [x[0] for x in result_df['thumbnail']]

    # yt_url
    result_df['yt_url'] = result_df['yt_url'].str.replace('https://www.youtube.com', '')
    result_df['yt_url'] = ['https://www.youtube.com'+x for x in result_df['yt_url']]

    # yt_id
    result_df['yt_id'] = [x.split('=') for x in result_df['yt_url']]
    for idx in result_df.index:
        yt_id = result_df.loc[idx, "yt_id"]
        # print(yt_id)
        if len(yt_id) == 1:
            result_df.loc[idx, "yt_id"]= yt_id[0].split("shorts/")[1]
        elif len(yt_id) == 2:
            result_df.loc[idx, "yt_id"] = yt_id[1]
        else:
            raise ValueError(f"yt_url을 확인해주세요\nyt_url: {result_df.loc[idx, 'yt_url']}\nyt_id: {yt_id}")

    result_df = result_df[['product_code', 'thumbnail', 'title', 'yt_id', 'yt_url', 'duration', 'youtuber']]
    
    return result_df

def upload():
    glamai_youtube_urls = db_glamai.get_tbl('glamai_youtube_urls').iloc[:, 1:]

    result_df = preprocessor()
    result_df.loc[:, 'regist_date'] = datetime.now()
    concat_df = pd.concat([glamai_youtube_urls, result_df], ignore_index=True)

    # sorting & dedup
    sorted_df = concat_df.sort_values(by="regist_date", ascending=False)
    subset = ['product_code', 'yt_url']
    dedup_df = sorted_df.drop_duplicates(subset=subset, keep='first', ignore_index=True)
    db_glamai.create_table(dedup_df, 'glamai_youtube_urls')
    
if __name__ == "__main__":
    upload()