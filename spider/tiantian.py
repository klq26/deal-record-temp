# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys

class tiantian(absspider):
    
    def __init__(self):
        self.app_name = ''
        self.input_file_name = ''
        pass
    
    def get_trade_list(self):
        pass
    
    def get_raw_record_list(self):
        pass

    def get_records(self):
        pass

if __name__ == "__main__":
    tt = tiantian()
    tt.get()