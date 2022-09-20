import datetime
from tqdm.auto import tqdm
import pandas as pd
from database.DB_manager import get_connection_to_mycelebs, engine_upload, truncate_table


def _update(vertical=None):
    conn = get_connection_to_mycelebs("glamai")
    curs = conn.cursor()
    truncate_table('glamai', f'{vertical}_product_info')

    data = pd.read_sql(f"""select product_code, item_no, url, price from glamai.sephora_{vertical}_data group by product_code, item_no;""", conn)
    curs.close()
    conn.close()

    data['regist_date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    engine_upload("glamai", f"{vertical}_product_info", data)


def check_data():
    qry = f"""
            select "sephora_eye_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_eye_data union
            select "sephora_eye_product_info" as vertical, count(*) cnt from glamai.eye_product_info union

            select "sephora_face_base_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_face_base_data union
            select "sephora_face_base_product_info" as vertical, count(*) cnt from glamai.face_base_product_info union

            select "sephora_lip_color_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_lip_color_data union
            select "sephora_lip_color_product_info" as vertical, count(*) cnt from glamai.lip_color_product_info union

            select "sephora_moisturizers_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_moisturizers_data union
            select "sephora_moisturizers_product_info" as vertical, count(*) cnt from glamai.moisturizers_product_info union

            select "sephora_masks_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_masks_data union
            select "sephora_masks_product_info" as vertical, count(*) cnt from glamai.masks_product_info union

            select "sephora_mens_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_mens_data union
            select "sephora_mens_product_info" as vertical, count(*) cnt from glamai.mens_product_info union

            select "sephora_treatments_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_treatments_data union
            select "sephora_treatments_product_info" as vertical, count(*) cnt from glamai.treatments_product_info union

            select "sephora_body_care_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_body_care_data union
            select "sephora_body_care_product_info" as vertical, count(*) cnt from glamai.body_care_product_info union

            select "sephora_cheek_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_cheek_data union
            select "sephora_cheek_product_info" as vertical, count(*) cnt from glamai.cheek_product_info union

            select "sephora_eye_care_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_eye_care_data union
            select "sephora_eye_care_product_info" as vertical, count(*) cnt from glamai.eye_care_product_info union


            select "sephora_wellness_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_wellness_data union
            select "sephora_wellness_product_info" as vertical, count(*) cnt from glamai.wellness_product_info union

            select "sephora_fragrance_men_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_fragrance_men_data union
            select "sephora_fragrance_men_product_info" as vertical, count(*) cnt from glamai.fragrance_men_product_info union

            select "sephora_fragrance_women_data" as vertical, count(distinct product_code, item_no) cnt from glamai.sephora_fragrance_women_data union
            select "sephora_fragrance_women_product_info" as vertical, count(*) cnt from glamai.fragrance_women_product_info
            ;
        """
    conn = get_connection_to_mycelebs("glamai")
    result = pd.read_sql(qry, conn)
    conn.close()
    return result

def update_product_info():
    verticals = [
        'eye', 'face_base', 'lip_color', 'moisturizers', 'masks', 'mens', 'treatments', 'body_care',
        'cheek', 'eye_care', 'wellness', 'fragrance_men', 'fragrance_women', 'cleansers',
    ]
    for vertical in tqdm(verticals):
        _update(vertical=vertical)

    result = check_data()
    return result