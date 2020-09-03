# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys

class huabao(absspider):

    def __init__(self):
        self.app_name = '华宝'
        self.input_file_name = '华宝证券.xlsx'
        pass

    def get_trade_list(self):
        # 不需要
        return None

    def get_raw_record_list(self):
        # 读取本地文件
        suppose_dtypes = {
            '成交日期': self.suppose_dtypes['date'], 
            '证券代码': self.suppose_dtypes['code'],
            '印花税': np.float64, 
            '过户费': np.float64, 
            '成交费': np.float64, 
            '委托编号': self.suppose_dtypes['unique_id']
        }
        df = pd.read_excel(self.input_file, dtype=suppose_dtypes)
        self.df_raw_record_list = df.copy()

    def get_records(self):
        # 整理数据
        # 799999 是创建账户，忽略
        ignore_codes = ['799999']
        df = self.df_raw_record_list[~(self.df_raw_record_list['证券代码'].isin(ignore_codes))]
        df = df.reset_index()
        df['id'] = df.index + 1
        df['date'] = df['成交日期'].apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[6:8])
        df['nav_acc'] = df['成交价格']
        df['account'] = '华宝'
        df['unique_id'] = df['委托编号']
        df['note'] = '无'
        df['fee'] = df['佣金'] + df['印花税'] + df['过户费'] + df['成交费']
        # '基金申购','托管转入' 都视为买入操作
        df = df.replace(['基金申购','托管转入'], ['买入','买入'])
        df['occur_money'] = df.apply(self.occur_money_calc, axis=1)
        # 补充一二三级分类
        df = pd.merge(df, self.cm.df_category, left_on='证券代码', right_on='基金代码', how='left')
        df = df.rename(columns={'成交时间':'time', '证券代码':'code', '证券名称':'name',
                                        '委托类别':'deal_type', '成交价格': 'nav_unit', '成交数量': 'volume',
                                        '发生金额': 'deal_money', '成本总计': 'fee', '一级分类': 'category1', 
                                        '二级分类': 'category2',  '三级分类': 'category3',  '分类ID': 'category_id', 
                                        '备注': 'note'})
        # 只要需要列
        df = df[record_keys()]
        self.df_results = df.copy()

    ##################
    # 转换函数
    ##################

    def occur_money_calc(self, x):
        if x['委托类别'] == '买入':
            return round(x['发生金额'] + x['fee'], 2)
        elif x['委托类别'] == '卖出':
            return round(x['发生金额'] - x['fee'], 2)
        pass

if __name__ == "__main__":
    hb = huabao()
    hb.get()