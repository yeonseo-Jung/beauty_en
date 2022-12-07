import os
import sys
import time
from tqdm.auto import tqdm

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)

from sephora_update.status import update_sephora_status
from sephora_update.sales import update_sephora_sale

verticals = ['treatments', 'masks', 'eye_care', 'body_care', 'mens', 'fragrance_men', 'fragrance_women', 'wellness', 'cleansers', 'face_base', 'eye', 'lip_color', 'moisturizers', 'cheek']
for vertical in tqdm(verticals):
    status_data_df = update_sephora_status(vertical)

time.sleep(100)
    
for vertical in tqdm(verticals):
    price_data = update_sephora_sale(vertical)