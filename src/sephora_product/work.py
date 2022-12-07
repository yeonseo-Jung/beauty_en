import os
import sys

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir, os.pardir))
    src  = os.path.join(root, 'src')
    sys.path.append(src)


#### 1st) Refinement
from sephora_product.refinement import Refinement
products = Refinement().update_refinement()


#### 2nd) Products By Subcategory
from sephora_product.product_keyword import ProductKeyword
upload_df = ProductKeyword().update_product_keyword()


#### 3rd) Update Vertical data
from sephora_product.vertical_data import VerticalData
VerticalData().update_vertical_data()


#### 4th) Update Best & New & Vegan & Organic
from sephora_product.best_new import UpdateBestSellerNew
from sephora_product.keywords import SephoraVeganOrganic
UpdateBestSellerNew().update_best_new()
SephoraVeganOrganic().update_keywords()


#### 5th) Review Date
from sephora_product.review_date import ReviewDate
new_product_list, data = ReviewDate().update_review_date()


#### 6th) Insert product info
from sephora_product.insert_product_info import update_product_info
result = update_product_info()


#### 7th) All product update
from sephora_product.all_product_update import update_all_product
data = update_all_product()


### Search Keywords Update
from sephora_product.search_keyword import update_search_keywords, db_distinction
total_df = update_search_keywords()
db_distinction()
