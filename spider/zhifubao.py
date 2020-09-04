# -*- coding: utf-8 -*-

import json

import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys

class zhifubao(absspider):
    
    def __init__(self):
        self.app_name = '支付宝'
        self.input_file_name = '康力泉_addition.json'
        pass
    
    def get_trade_list(self):
        # 不需要
        return None
    
    def get_raw_record_list(self):
        # 不需要
        return None

    def get_records(self):
        with open(self.input_file, 'r', encoding='utf-8') as f:
            df = pd.DataFrame([(pd.Series(x)) for x in json.loads(f.read())])
            self.df_results = df.copy()

if __name__ == "__main__":
    zfb = zhifubao()
    zfb.get()