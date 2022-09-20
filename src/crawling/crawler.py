import os
import re
import sys
import time
import json
import requests
# import pickle
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

# Exception Error Handling
import socket
import warnings
warnings.filterwarnings("ignore")

def get_url(url, window=False, image=False):
    ''' Set up webdriver, useragent & Get url '''
    
    wd = None
    socket.setdefaulttimeout(30)
    error = []
    attempts = 0 # url parsing 시도횟수
    # 10번 이상 parsing 실패시 pass
    while attempts < 10:
        try:  
            attempts += 1
            # user agent
            options = Options() 
            userAgent = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
            options.add_argument('window-size=1920x1080')
            options.add_argument("--disable-gpu")
            options.add_argument('--disable-extensions')
            options.add_argument(f'user-agent={userAgent}')
            
            if not window:
                options.add_argument('headless')
            if not image:
                options.add_argument('--blink-settings=imagesEnabled=false')

            # web driver 
            wd = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=options)
            wd.get(url)
            wd.implicitly_wait(5)
            break

        # 예외처리
        except Exception as e:
            print(f'\n\nError: {str(e)}\n\n')
            
            time.sleep(300)
            try:
                wd.quit()
            except:
                pass
            wd = None
    return wd
    
def scroll_down(wd, sleep_time, check_count):
    ''' page scroll down '''
    
    cnt = 0
    while True:
        height = wd.execute_script("return document.body.scrollHeight")
        wd.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
        time.sleep(sleep_time)
        cnt += 1
        if cnt == check_count:
            break        
    return wd

def get_headers():
    userAgent = generate_user_agent(os=('mac', 'linux'), navigator='chrome', device_type='desktop')
    headers = {
        "user-agent": userAgent,
        "Accept": "application/json",
    }
    return headers

def json_iterator(url, iterations=5, headers=True):
    
    if headers:
        headers = get_headers()
    else:
        headers = None
    cnt = 0
    res_data = None
    while cnt <= iterations:
        try:
            res = requests.get(url, headers=headers)
            res_data = json.loads(res.text)
            break
        except Exception as e:
            print(f'Error: {e}')
        cnt += 1
        time.sleep(100)
        
    return res_data