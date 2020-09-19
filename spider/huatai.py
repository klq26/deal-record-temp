# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys

class huatai(absspider):

    def __init__(self):
        self.app_name = '华泰'
        pass

    def get_trade_list(self):
        # 不需要
        return None

    def get_raw_record_list(self):
        # 历史文件
        self.input_file_name = '华泰证券2015-2019.xlsx'
        df_raw_2015 = pd.read_excel(self.input_file, dtype={'发生日期': str, '证券代码': str, '成交数量': np.float64})
        df_raw_2015['发生日期'] = df_raw_2015['发生日期'].apply(lambda x: str(x)[0:10])
        # 2020 年开始新文件
        self.input_file_name = '华泰证券.xlsx'
        df_raw_current = pd.read_excel(self.input_file, dtype={'发生日期': str, '证券代码': str, '成交数量': np.float64})
        # 四费合一
        df_raw_current['佣金'] = df_raw_current['佣金'] + df_raw_current['印花税'] + df_raw_current['过户费'] + df_raw_current['其他费']
        df_raw_current = df_raw_current[['流水号', '发生日期', '证券名称', '证券代码', '买卖标志', '业务名称', '成交价格', '成交数量', '发生金额',
            '剩余金额', '佣金', '股东代码', '备注']]
        df_raw_current['发生日期'] = df_raw_current['发生日期'].apply(lambda x: x[0:4] + '-' + x[4:6] + '-' + x[6:8])
        df = pd.concat([df_raw_2015, df_raw_current], ignore_index=True)
        self.df_raw_record_list = df.copy()
        pass

    def get_records(self):
        # 整理数据
        df_raw = self.df_raw_record_list.copy()
        # 基本上，证券名称为 " " 的记录，不是银行存取款，就是看也看不懂的。忽略吧
        df = df_raw[~(df_raw['证券名称'] == ' ')]
        # 货币基金
        df_money_fund = df[df['证券名称'].isin(['银华日利', '紫金货币', '天天发1', 'GC007', 'Ｒ-001', 'GC001', 'Ｒ-003', '现金添富', '华宝添益', '添富快线'])]
        # 非货币基金
        df = df[~df.index.isin(df_money_fund.index)]
        # df[df['业务名称'].isin(['开放基金赎回返款', '开放基金赎回','股息入帐'])]
        df = df.reset_index(drop=True)
        df['id'] = df.index + 1
        df['date'] = df['发生日期']
        df['time'] = '9:30:00'
        df['code'] = df['证券代码']
        df['name'] = df['证券名称']
        df['deal_type'] = df['业务名称'].apply(lambda x: self.deal_type_calc(x))
        df['nav_unit'] = df['成交价格']
        df['nav_acc'] = df['成交价格']
        df['volume'] = np.abs(df['成交数量'])
        df['fee'] = df['佣金']
        df['occur_money'] = df.apply(self.occur_money_calc, axis=1)
        df['deal_money'] = df.apply(self.deal_money_calc, axis=1)
        df['account'] = '华泰'
        df['unique_id'] = df.apply(self.unique_id_calc, axis=1)
        df['note'] = '无'
        # 补充一二三级分类
        df = pd.merge(df, self.cm.df_category, left_on='证券代码', right_on='基金代码', how='left')
        df = df.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        # 
        df = df[record_keys()]
        self.df_results = df.copy()
        pass

    def deal_money_calc(self, x):
        if x['deal_type'] == '分红':
            return round(np.abs(x['发生金额']), 2)
        else:
            money = round(np.abs(x['nav_unit'] * x['volume']), 2)
            return money

    def adjust_dates(self):
        # 不需要
        pass

    ##################
    # 转换函数
    ##################

    def occur_money_calc(self, x):
        money = round(np.abs(x['nav_unit'] * x['volume']), 2)
        if x['deal_type'] == '买入':
            return round(money + x['fee'], 2)
        elif x['deal_type'] == '卖出':
            return round(money - x['fee'], 2)
        elif x['deal_type'] == '分红':
            return round(np.abs(x['发生金额']) - x['fee'], 2)
        else:
            return 0.0
        
    def deal_type_calc(self, x):
        if x in ['买入', '证券买入']:
            return '买入'
        elif x in ['卖出', '证券卖出', '开放基金赎回']:
            return '卖出'
        elif x in ['开放基金赎回返款', '股息入帐']:
            return '分红'
        pass

    def unique_id_calc(self, x):
        return x['发生日期'] + '_' + str(x['流水号'])

if __name__ == "__main__":
    ht = huatai()
    # ht.get()
    ht.get()