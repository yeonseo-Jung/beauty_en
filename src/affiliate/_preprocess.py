import os
import re
import sys
import ast
from tqdm.auto import tqdm

import numpy as np
import pandas as pd

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

class TitlePreProcess:
    
    def __init__(self):
        
        # 제거해야 할 단어 정규식 표현
        # self.extract_reg = [spf, pa]

        # 유지해야 할 단어 정규식 
        spf = re.compile('spf\s*[0-9]*[+]*')
        pa = re.compile('pa\s*[0-9]*[+]+')        
        volume_ml = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[m]*l')
        volume_kg = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[mk]*g')
        volume_oz = re.compile('[x]*\s*[0-9]*[.]?[0-9]+\s*[fl]*\s*oz')
        num_0 = re.compile('[a-z]*\s*[0-9]+\s*호')
        num_1 = re.compile('#\s*[a-z]*\s*[0-9]+')
        num_2 = re.compile('[n]+[o]+[.]?\s*[0-9]+')
        self.keep_wd_reg = [spf, pa, volume_ml, volume_kg, volume_oz, num_0, num_1, num_2]
        self.keep_wd_list = ['spf', 'pa', 'volume_ml', 'volume_kg', 'volume_oz', 'num_0', 'num_1', 'num_2']

        # # 불용어 패턴 정규식
        # stp_pattern = [
        # '[총x]*\s*[0-9]*[.]?[0-9]+\s*[eaEA]+\s*[0-9]*\s*[씩]?\s*[더]?',
        # '[총x]*\s*[0-9]*[.]?[0-9]+\s*[매]+[입]?\s*[0-9]*\s*[씩]?\s*[더]?',
        # '[총x]*\s*[0-9]*[.]?[0-9]+\s*[개]+[입]?\s*[0-9]*\s*[씩]?\s*더?',
        # '[총x]*\s*[0-9]*[.]?[0-9]+\s*[팩]+\s*[0-9]*\s*[씩]?\s*[더]?',
        # '[총x]*\s*[0-9]*[.]?[0-9]+\s*[장]+\s*[0-9]*\s*[씩]?\s*[더]?',
        # '[총x]*\s*[0-9]*[.]?[0-9]+\s*[p]+\s*[0-9]*\s*[씩]?\s*[더]?',
        # ]
        # stp_pattern_reg = []
        # for pattern in stp_pattern:
        #     reg = re.compile(f'{pattern}')
        #     stp_pattern_reg.append(reg)
        # self.stp_pattern_reg = stp_pattern_reg
        
    def extract_info(self, title):    
        
        # '''제거 할 문자 추출'''
        # for r in self.extract_reg:
        #     r_ = r.findall(title)

        #     if len(r_) == 0:
        #         pass
            
        #     elif len(r_) == 1:
        #         title = title.replace(r_[0], ' ')
                
        #     else:
        #         for elm in r_:
        #             title = title.replace(elm, ' ')

        '''유지 할 문자 추출'''
        keep_wd_dict = {}
        i = 0
        for r in self.keep_wd_reg:
            r_ = r.findall(title)

            if len(r_) == 0:
                pass

            elif len(r_) == 1:
                keep_wd = r_[0]
                title = title.replace(keep_wd, ' ')
                
                keep_wd = keep_wd.replace(' ', '')
                keep_wd_dict[self.keep_wd_list[i]] = keep_wd

            else:
                for elm in r_:
                    title = title.replace(elm, ' ')
                    
                keep_wd = max(r_, key=lambda x: len(x)) # 길이가 가장 긴 원소 채택
                keep_wd = keep_wd.replace(' ', '')
                keep_wd_dict[self.keep_wd_list[i]] = keep_wd
                
            i += 1
            
        title = re.sub(' +', ' ', title)
        title = title.strip()
        
        if title == '':
            title = np.nan

        return title, keep_wd_dict

    def remove_stp_pattern(self, title):           
        '''불용어 패턴 제거 및 정규화'''
        
        # for pattern in self.stp_pattern_reg:
        #     title = pattern.sub(' ', title)

        # '''상품명에서 한글, 영문, 숫자, .만 추출'''
        title = re.sub('[^a-z0-9.]', ' ', title)
        title = re.sub(' +', ' ', title)
        title = title.strip()
        
        if title == '':
            title = np.nan

        return title

    def remove_dup_words(self, title):
        '''타이틀 중복 토큰 제거'''

        word_sp = title.split(' ')
        org_idx = list(range(len(word_sp)))
        
        # 중복단어 index
        dup_index = [i for i, v in enumerate(word_sp) if v in word_sp[:i]]
        drop_index = list(set(dup_index))
        for i in drop_index:
            org_idx.remove(i)

        title_dedup = ' '.join([word_sp[i] for i in org_idx])
        
        if title == '':
            title = np.nan
            
        return title_dedup

    def insert_keep_wd(self, title, keep_wd_dict):
        '''유지 할 문자 삽입'''
        
        word_sp = title.split(' ')
        keep_wd_set = list(set(keep_wd_dict.values()))

        title_sp = word_sp + keep_wd_set
        
        title_keep_wds = ' '.join(title_sp)
        
        title_keep_wds = re.sub(' +', ' ', title_keep_wds)
        title_keep_wds = title_keep_wds.strip()
        
        if title_keep_wds == '':
            title_keep_wds = np.nan
            
        return title_keep_wds

    def title_preprocessor(self, title, brand):
        ''' preprocessing product name '''
        
        title = str(title).lower()
        brand = str(brand).lower()
        title = title.replace('á', 'a').replace('é', 'e').replace('ô', 'o')
        brand = brand.replace('á', 'a').replace('é', 'e').replace('ô', 'o')
        title_ = title.replace(brand, ' ')
        title_ = re.sub(' +', ' ', title_).strip()

        '''상품명에서 상품 정보 추출'''
        if str(title_) == '':
            return title, {}
        else:
            return_data = self.extract_info(title_)
            title_0 = return_data[0]
            keep_wd_dict = return_data[1]

        '''불용어 패턴 제거 및 한글 추출'''
        if str(title_0) == 'nan':
            return title_, keep_wd_dict
        else:
            title_1 = self.remove_stp_pattern(title_0)

        if str(title_1) == 'nan':
            return title_0, keep_wd_dict
        else:
            return title_1, keep_wd_dict
        
        # if str(title_2) == 'nan':
        #     return title_1, keep_wd_dict
        # else:
        #     return title_2, keep_wd_dict
        
# tp = TitlePreProcess()
# def preprocess_titles(df):
#     preprocessed = []
#     for idx in tqdm(df.index):
#         title = df.loc[idx, 'product_name']
#         brand = df.loc[idx, 'brand']
#         _title, keep_wds = tp.title_preprocessor(title, brand)
        
#         preprocessed.append([_title, keep_wds])
#     preprocessed_df = pd.DataFrame(preprocessed, columns=['preprocessed', 'keep_wds'])
#     df_concat = pd.concat([df, preprocessed_df.loc[:, ['preprocessed', 'keep_wds']]], axis=1)
    
#     return df_concat

# preprocessed_df = preprocess_titles(df)
# for idx in preprocessed_df.index:
#     title = preprocessed_df.loc[idx, 'product_name']
#     brand = preprocessed_df.loc[idx, 'brand']
#     preprocessed = preprocessed_df.loc[idx, 'preprocessed']
#     keep_wds = preprocessed_df.loc[idx, 'keep_wds']
    
#     print(
#         f'\n\n\
#         \n\t * Brand              : {brand}\
#         \n\t * Original title     : {title}\
#         \n\t * Preprocessed title : {preprocessed}\
#         \n\t * keep words         : {keep_wds}'
#     )

tp = TitlePreProcess()
def preprocess_titles(df: pd.DataFrame) -> pd.DataFrame:
    '''상품명, 브랜드명 전처리 함수
    필수 컬럼: "product_name", "brand"
    '''
    
    preprocessed = []
    for idx in tqdm(df.index):
        title = df.loc[idx, 'product_name']
        brand = df.loc[idx, 'brand']
        _title, keep_wds = tp.title_preprocessor(title, brand)
        
        # preprocessed.append([_title, keep_wds])
        df.loc[idx, 'preprocessed'] = _title
        for key in keep_wds.keys():
            df.loc[idx, key] = keep_wds[key]
    
    return df

# 중복 구하기
def dup_check(df, subset, keep=False, sorting=False):
    
    if sorting:    
        return df[df.duplicated(subset=subset, keep=keep)].sort_values(by=subset, ignore_index=True)
    else:
        return df[df.duplicated(subset=subset, keep=keep)].reset_index(drop=True)
    
def subtractor(df_a, df_b, subset):
    # 차집합 구하기: df_a - df_b 

    # 중복값 구하기
    dup_df = dup_check(pd.concat([df_a, df_b]), subset=subset)
    dedup_df = dup_df.drop_duplicates(subset=subset, keep='first')

    # 차집합 
    subtract_df = pd.concat([df_a, dedup_df]).drop_duplicates(subset=subset, keep=False)
    
    return subtract_df