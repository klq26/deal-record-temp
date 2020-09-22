# -*- coding: utf-8 -*-

import os
import json
import time, datetime

import grequests
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys
from analysis.jqadapter import jqadapter

class qieman(absspider):
    
    def __init__(self, uname=None):
        self.app_name = '且慢'
        self.headers = {}
        self.name_mapping = {'klq': '康力泉', 'ksh': '康世海'}
        self.plan_list = []
        self.set_user_id(uname)
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        pass
    
    def set_user_id(self, uname = None):
        """
        判断用户是谁，应该读取哪个配置文件
        """
        # 名字
        s_name = ''
        if uname and uname in ['klq', 'ksh']:
            s_name = uname
        else:
            s_name = 'klq'
        self.user_name = self.name_mapping[s_name]

        # plan
        plan150_id = u'CA8UKLYHA67WPK'
        planS_id = u'CA8FCJKFPANTP2'
        planWenWen_id = u'CA942R8128PFE7'
        if s_name == 'klq':
            self.headers = self.wm.get_qieman_klq()
            self.plan_list = [{'name': '150份', 'value': plan150_id, 'poName': '长赢指数投资计划-150份'}, 
                                {'name': 'S定投', 'value': planS_id, 'poName': '长赢指数投资计划-S定投'}]
        elif s_name == 'ksh':
            self.headers = self.wm.get_qieman_ksh()
            self.plan_list = [{'name': '稳稳的幸福', 'value': planWenWen_id}]
        pass

    def get_trade_list(self):
        # 获取所有计划的交易列表
        trade_list_url = u'https://qieman.com/pmdj/v2/orders?capitalAccountId={0}&page=0&size=500'
        tasks = [grequests.get(trade_list_url.format(x['value']), headers=self.headers, callback=self.grequests_get_callback) for x in self.plan_list]
        response_list = grequests.map(tasks, size=3, exception_handler=self.grequests_exception_handler)
        trade_list = []
        for response in response_list:
            [trade_list.append(x) for x in response.json()['content']]
        # 输出原始交易列表（不包含 record）
        df = pd.DataFrame([pd.Series(x) for x in trade_list])
        df['acceptTime'] = df.acceptTime.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x/1000))))
        df = df.fillna(value={'txnDay':0})
        df['txnDay'] = df.txnDay.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(x/1000))))
        # 且慢分红记录没有详情，忽略吧
        df = df[~(df['hasDetail'] == False)]
        df = df.sort_values(['acceptTime'])
        df = df.reset_index(drop=True)
        self.df_trade_list = df.copy()
        pass
    
    def get_raw_record_list(self):
        # 获取原始交易记录
        trade_detail_url = u'https://qieman.com/pmdj/v2/orders/{0}'
        tasks = [grequests.get(trade_detail_url.format(x), headers=self.headers, callback=self.grequests_get_callback) for x in self.df_trade_list.orderId.tolist()]
        response_list = grequests.map(tasks, size=3, exception_handler=self.grequests_exception_handler)
        trade_detail_list = []
        for response in response_list:
            jsonData = response.json()
            for x in jsonData['compositionOrders']:
                x['planName'] = jsonData['po']['poName']
                trade_detail_list.append(x)
        # 生成 raw 原始成交记录
        df = pd.DataFrame([pd.Series(x) for x in trade_detail_list])
        df['acceptTime'] = df.acceptTime.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df['orderConfirmDate'] = df.orderConfirmDate.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        # 去掉尚未完成的记录
        df = df[df['uiConfirmStatusName'] == '成功']
        df = df.sort_values(['acceptTime'])
        df = df.reset_index(drop=True)
        # 转托管逻辑过于复杂，之后但凡有此类操作，直接读取 input 文件夹下的修正成交记录
        df = df[~(df['uiOrderCodeName'] == '场外转托管')]
        # 把且慢稳稳的幸福的转换至都分拆放到 addition.json 里面了，这块太难搞了，以后应该也不会买且慢组合了，就不写逻辑了。
        df = df[~(df['uiOrderCodeName'].str.contains('转换至'))]
        self.df_raw_record_list = df.copy()
        pass

    def get_records(self):
        # 读取逻辑过于复杂，自己补充的交易记录（分红、转换）
        self.input_file_name = '{0}_addition.json'.format(self.user_name)
        df_addtion_record = None
        with open(self.input_file, 'r', encoding='utf-8') as f:
            df_addtion_record = pd.DataFrame([(pd.Series(x)) for x in json.loads(f.read())])
        
        # 每一条成交记录
        # 所有的操作 ['盈米宝购买', '赎回', '赎回到盈米宝', '场外转托管']
        df = self.df_raw_record_list.copy()
        df['id'] = self.df_raw_record_list.reset_index().index + 1
        # TODO 15 点逻辑
        df['date'] = df['acceptTime'].apply(lambda x: str(x)[0:10])
        df['time'] = df['acceptTime'].apply(lambda x: str(x).split(' ')[1])
        df['code'] = df['fund'].apply(lambda x: json.loads(str(x).replace('\'','\"'))['fundCode'])
        df['name'] = df['fund'].apply(lambda x: json.loads(str(x).replace('\'','\"'))['fundName'])
        df['deal_type'] = df.apply(self.deal_type_calc, axis=1)
        df['volume'] = df['uiShare']
        df['fee'] = df['fee']
        df['nav_unit'] = df['nav']
        df['nav_acc'] = df['nav']
        df['deal_money'] = df.apply(self.deal_money_calc, axis=1)
        df['occur_money'] = df.apply(self.occur_money_calc, axis=1)
        df['account'] = '{0}_且慢'.format(self.user_name)
        # 补充额外记录
        if len(df_addtion_record) > 0:
            df = pd.concat([df, df_addtion_record], sort=False)
        # 补充一二三级分类
        df = pd.merge(df, self.cm.df_category, left_on='code', right_on='基金代码', how='left')
        df = df.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        df['unique_id'] = df.apply(self.unique_id_calc, axis=1)
        # 用三级分类表中的名称统一各大基金 APP 中的名称
        df['name'] = df['基金名称']
        df['note'] = df.apply(self.note_calc, axis=1)
        df = df.sort_values(['date','time'])
        df = df.reset_index()
        df['id'] = df.index + 1       
        df = df[record_keys()]
        self.df_results = df.copy()
        pass

    def adjust_dates(self):
        """
        对账单中的日期是真实申请的时间，如果时间晚于 15:00:00，净值日期应该后搓一天
        """
        jq = jqadapter()
        series_all_days = jq.get_trade_day_info()
        df_record = self.df_results.copy()
        adjust_dates = []
        adjust_times = []
        for x in df_record.itertuples():
            hour = int(x.time[0:2])
            if hour >= 15:
                next_trade_day = series_all_days[series_all_days > x.date][0]
                adjust_dates.append(next_trade_day)
                adjust_times.append('14:59:00')
            else:
                adjust_dates.append(x.date)
                adjust_times.append(x.time)
        df_record['date'] = adjust_dates
        df_record['time'] = adjust_times
        df_record['code'] = df_record['code'].apply(lambda x: str(x).zfill(6))
        self.df_results = df_record.copy()
        pass

    # grequests
    def grequests_exception_handler(self, request, exception):
        print("Request failed: {0}".format(exception))

    def grequests_get_callback(self, request, *args, **kwargs):
        data_length = len(request.text)
        if data_length < 20:
            print('{0} 失败，返回长度小于 20 字符，退出'.format(request.url))
            exit(1)
        print(request.url, request, 'data length: ', data_length)

    def deal_type_calc(self, val):
        x = val['uiOrderCodeName']
        if u'赎回' in x:
            return '卖出'
        elif u'购买' in x or u'申购' in x:
            return '买入'
        elif u'转托管' in x:
            return '卖出'
        else:
            return x

    def deal_money_calc(self, x):
        volume = x['uiShare']
        money = x['uiAmount']
        fee = x['fee']
        if x['deal_type'] in ['买入']:
            deal_money = round(money - fee, 4)
        elif x['deal_type'] == '卖出':
            deal_money = round(money, 4)
        else:
            deal_money = money
        return deal_money
        
    def occur_money_calc(self, x):
        volume = x['uiShare']
        money = x['uiAmount']
        fee = x['fee']
        if x['deal_type'] in ['买入']:
            occur_money = round(money, 4)
        elif x['deal_type'] == '卖出':
            occur_money = round(money - fee, 4)
        else:
            occur_money = money
        return occur_money

    def unique_id_calc(self, x):
        if pd.isna(x['orderId']):
            return x['unique_id']
        else:
            return x['orderId']

    def note_calc(self, x):
        if pd.isna(x['orderId']):
            return x['note']
        else:
            return x['planName'] + '_' + x['uiOrderCodeName'] + '_orderid=' + x['orderId']

if __name__ == "__main__":
    qm = qieman()
    # 设置用户
    qm.set_user_id('klq')
    # qm.set_user_id('ksh')
    qm.get()