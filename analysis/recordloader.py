# -*- coding: utf-8 -*-

import os
from os import path
import sys
# import time
# from datetime import datetime
# from datetime import timedelta

import pandas as pd
import numpy as np

# 加根目录到 sys.path
project_folder = os.path.abspath(os.path.join(path.dirname(__file__), os.pardir))
if project_folder not in sys.path:
    sys.path.append(project_folder)

from category.categories import categories
from spider.record import record_keys

class recordloader:
    """
    负责按真实用户（康力泉、父母）为单元，获取完整的交易记录等信息
    """
    def __init__(self):
        self.folder = path.abspath(path.dirname(__file__))
        self.name_mapping = {'klq': [u'康力泉'], 'parents': [u'李淑云', u'康世海']}
        self.user_names = [u'康力泉']
        self.df_records = None
        self.suppose_dtypes = {'id':np.int64, 'date':str, 'time':str, 'code':str, 
                               'name':np.object, 'deal_type':np.object, 'nav_unit':np.float64,
                               'nav_acc':np.float64,'volume':np.float64, 'deal_money':np.float64, 
                               'fee':np.float64,'occur_money':np.float64, 'account':np.object,
                               'category1':np.object, 'category2':np.object, 'category3':np.object, 
                               'category_id':np.object,'note':np.object}

        pass

    def set_user_id(self, uname = None):
        """
        判断用户是谁，应该读取哪个配置文件
        """
        s_name = ''
        if uname and uname in ['klq', 'parents']:
            s_name = uname
        else:
            s_name = 'klq'
        self.user_names = self.name_mapping[s_name]
        # if s_name == 'klq':
        #     pass
        # elif s_name == 'parents':
        #     pass
        # 初始化一下数据
        self.get_records()
        pass

    def get_records(self):
        """
        获取父母或自己的完整交易记录
        """
        file_paths = []
        output_path = path.join(project_folder, 'output')
        for root, dirs, files in os.walk(output_path):
            for f in files:
                # 最终结果产出文件（忽略中间过程文件）
                if f.startswith(u'03'):
                    # 如果是父母，要同时包含 “李淑云” 和 “康世海” 的数据
                    should_select = False
                    for name in self.user_names:
                        should_select = should_select or name in f
                    if should_select:
                        file_paths.append(path.join(root, f))
        # 读取所有相关文件
        dfs = [pd.read_excel(file_path, index_col=0, dtype=self.suppose_dtypes) for file_path in file_paths]
        # 整合到一张表
        df = pd.concat(dfs)
        # 规整
        df = df.sort_values(['date','time','category_id','code'])
        df = df.reset_index(drop=True)
        df.id = df.index + 1
        output_name = '康力泉'
        # 父母是两人
        if len(self.user_names) > 1:
            output_name = '父母'
        # 根据买入卖出调整份额的正负
        def volume_calc(x):
            op = x['deal_type']
            volume = np.abs(x['volume'])
            if op in ['卖出', '托管转出']:
                return -volume
            elif op in ['买入', '分红', '托管转如']:
                return volume
            else:
                return volume
        df.volume = df.apply(volume_calc, axis=1)
        df.to_excel(path.join(project_folder, 'output', '{0}_全部记录.xlsx'.format(output_name)))
        self.df_records = df.copy()
        return self.df_records

    def get_all_fund_unique_codes(self):
        """
        获取所有交易记录中的基金唯一代码（用于拿分红，拿历史净值等操作）
        """
        df = self.df_records[~(self.df_records['category3'] == '股票')]
        return df.code.unique().tolist()

    def get_all_users_combine_records(self):
        """
        获取全部用户的所有交易记录，放入一张表，用来取基金净值
        """
        # 获取康力泉所有记录
        self.set_user_id('klq')
        df1 = self.get_records()
        # 获取父母所有记录
        self.set_user_id('parents')
        df2 = self.get_records()
        df = pd.concat([df1, df2], ignore_index=True)
        df = df.sort_values(['date','time','category_id','code'])
        df = df.reset_index(drop=True)
        # 家庭整体交易记录
        df.id = df.index + 1
        df.to_excel(path.join(project_folder, 'output', '全部记录.xlsx'))
        return df

if __name__ == "__main__":
    r = recordloader()
    r.set_user_id('klq')
    r.get_records()
    r.set_user_id('parents')
    r.get_records()
    r.get_all_users_combine_records()