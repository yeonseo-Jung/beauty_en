import os
import sys
import time
import traceback
from datetime import datetime
import pandas as pd

if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    root = sys._MEIPASS
else:
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    root = os.path.abspath(os.path.join(cur_dir, os.pardir))

from database.access import AccessDatabase
tbl_cache = os.path.join(root, 'tbl_cache')
conn_path = os.path.join(root, 'conn.txt')

class Errors:
    def __init__(self):
        # db 연결
        self.db = AccessDatabase('jangho')
        
    def errors_log(self, url=''):
        tb = traceback.format_exc()
        print(tb)
        _datetime = pd.Timestamp(datetime.today())
        table = 'error_log'
        fields = ('url', 'traceback', 'error_date')
        values = (url, tb, _datetime)
        
        while True:
            try:
                self.db._insert(table, fields, values)
                break
            except Exception as e:
                self.errors_log()
                time.sleep(60)
                self.db = AccessDatabase('jangho')