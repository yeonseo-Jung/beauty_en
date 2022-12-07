import os
import sys
import datetime

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

from database.conn import AccessDatabase

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

    def get_new_product(self):
        sql = f'''
            select distinct product_code from (
                select distinct product_code from sephora_eye_data union 
                select distinct product_code from sephora_face_base_data union
                select distinct product_code from sephora_lip_color_data union 
                select distinct product_code from sephora_moisturizers_data union
                select distinct product_code from sephora_cheek_data union
                select distinct product_code from sephora_treatments_data union
                select distinct product_code from sephora_masks_data union
                select distinct product_code from sephora_eye_care_data union
                select distinct product_code from sephora_body_care_data union 
                select distinct product_code from sephora_mens_data union 
                select distinct product_code from sephora_wellness_data union 
                select distinct product_code from sephora_hair_styling_data union 
                select distinct product_code from sephora_curly_hair_care_data union 
                select distinct product_code from sephora_hair_dye_root_touchups_data union 
                select distinct product_code from sephora_shampoo_conditioner_data union 
                select distinct product_code from sephora_hair_treatments_data union 
                select distinct product_code from sephora_bath_shower_data union 
                select distinct product_code from sephora_cleansers_data union 
                select distinct product_code from sephora_fragrance_women_data union 
                select distinct product_code from sephora_fragrance_men_data union 
                select distinct product_code from sephora_candles_home_scents_data union 
                select distinct product_code from sephora_sun_care_data union 
                select distinct product_code from sephora_self_tanner_data union 
                select distinct product_code from sephora_makeup_brushes_applicators_data union 
                select distinct product_code from sephora_accessories_data union 
                select distinct product_code from sephora_nail_polish_treatments_data union 
                select distinct product_code from sephora_hair_tools_data
            ) as a
            where not exists (
            select distinct product_code from sephora_review_date_re as b
            where a.product_code = b.product_code
            );
        '''
        self.ds_cur.execute(sql)
        data = self.ds_cur.fetchall()
        return data

    def _insert(self, data):
        sql = '''
            insert into sephora_review_date_re(product_code, write_time, regist_date, update_date)
            values(%s, %s, %s , %s)
        '''
        self.ds_cur.executemany(sql, data)

    def update_review_date(self):
        today = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        new_product_list = self.get_new_product()
        data = [[product['product_code'], '1970-01-01 00:00:00', today, today] for product in new_product_list]
        self._insert(data)
        self.__close__()

        print("sephora_review_date_re 업데이트 완료")
        
        return new_product_list, data