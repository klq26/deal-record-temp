# -*- coding: utf-8 -*-

import os
import sys
import json
import time, datetime
from datetime import datetime
from datetime import timedelta

import grequests
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys
from analysis.jqadapter import jqadapter
from category.fundinfo import fundinfo

# 每次抓包需要修改
qtk_klq = u'4a880ad663fc401aa625553727ccd5c5'
qtk_lsy = u'881e21589fe94a00bfc4ef8c192509cf'

class tiantian(absspider):

    def __init__(self, uname=None):
        self.app_name = '天天'
        self.date_type = '确认日' # 日期需要整体提前一个交易日
        self.headers = {}
        self.name_mapping = {'klq': '康力泉', 'lsy': '李淑云'}
        self.set_user_id(uname)
        # 采用抓取“我的对账单”来获取数据
        self.start_year = 1970
        self.start_month = 1
        # 我和老妈的不太一样
        self.server_domain = 'trade'
        # 不可或缺的参数，抓取 bi.aspx 三个接口时，也需要抓取这个参数
        self.qkt = qtk_klq
        # 分红记录要保留
        self.df_divid = None
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        pass
    
    def set_user_id(self, uname = None):
        """
        判断用户是谁，应该读取哪个配置文件
        """
        # 名字
        s_name = ''
        if uname and uname in ['klq', 'lsy']:
            s_name = uname
        else:
            s_name = 'klq'
        self.user_name = self.name_mapping[s_name]
        # 额外数据
        if s_name == 'klq':
            self.headers = self.wm.get_tiantian_klq()
            self.start_year = 2016
            self.start_month = 5
            self.server_domain = 'trade'
            self.qkt = qtk_klq
        elif s_name == 'lsy':
            self.headers = self.wm.get_tiantian_lsy()
            self.start_year = 2018
            self.start_month = 1
            self.server_domain = 'trade7'
            self.qkt = qtk_lsy
        self.input_file_name = '{0}_addition.json'.format(self.user_name)

    def get_trade_list(self):
        # 不需要
        pass
    def get_raw_record_list(self):
        # qkt 参数不可或缺，抓 cookie 的时候搜索 bi.aspx
        # 持仓
        url_hold = u'https://{0}.1234567.com.cn/SearchHandler/bi.aspx?callback=callback&type=billhold&qkt={1}&ttype=month&year={2}&data={3}'
        # 交易
        url_trade = u'https://{0}.1234567.com.cn/SearchHandler/bi.aspx?callback=callback&type=billtrade&qkt={1}&ttype=month&year={2}&data={3}'
        # 分红
        url_divid = u'https://{0}.1234567.com.cn/SearchHandler/bi.aspx?callback=callback&type=billdivid&qkt={1}&ttype=month&year={2}&data={3}'
        # 数据还是希望包含本月
        today = datetime.today()
        end_month = today.month# .strftime('%Y-%m-%d')
        if end_month == 12:
            end_month = 1
        else:
            end_month += 1
        # 下个月 1 日
        next_month_start_date = today.replace(month=end_month, day=1)
        ts_range = pd.date_range(start='{0}-{1}-01'.format(self.start_year, self.start_month), end=next_month_start_date.strftime('%Y-%m-%d'), freq='1M').tolist()
        date_range = []
        [date_range.append({'year': x.year, 'month': x.month}) for x in ts_range]
        hold_list = []
        trade_list = []
        divid_list = []

        for i, date_dict in enumerate(date_range):
            print('整理 {0}年{1}月'.format(date_dict['year'], str(date_dict['month']).zfill(2)))
            url1 = url_hold.format(self.server_domain, self.qkt, date_dict['year'], date_dict['month'])
            url2 = url_trade.format(self.server_domain, self.qkt, date_dict['year'], date_dict['month'])
            url3 = url_divid.format(self.server_domain, self.qkt, date_dict['year'], date_dict['month'])
            urls = [url1, url2, url3]
            tasks = [grequests.get(x, headers = self.headers, callback=self.grequests_get_callback) for x in urls]
            resp_list = grequests.map(tasks, size=1, exception_handler=self.grequests_exception_handler)
            for resp in resp_list:
                if u'billhold' in resp.request.url:
                    data_list = json.loads(resp.text.replace(');','').replace('callback(',''))['result']['datas']
                    for x in data_list:
                        # 给持仓数据补上年份
                        x['date'] = str(date_dict['year']) + '-' + x['date']
                    [hold_list.append(pd.Series(x)) for x in data_list]
                if u'billtrade' in resp.request.url:
                    data_list = json.loads(resp.text.replace(');','').replace('callback(',''))['result']['datas']
                    [trade_list.append(pd.Series(x)) for x in data_list]
                if u'billdivid' in resp.request.url:
                    data_list = json.loads(resp.text.replace(');','').replace('callback(',''))['result']['datas']
                    [divid_list.append(pd.Series(x)) for x in data_list]
            # if i == 5:
            #     break
        df_hold = pd.DataFrame(hold_list)
        df_trade = pd.DataFrame(trade_list)
        df_divid = pd.DataFrame(divid_list)
        df_hold.to_excel(os.path.join(self.output_folder, '{0}_hold.xlsx'.format(self.user_name)))
        df_trade.to_excel(os.path.join(self.output_folder, '{0}_trade.xlsx'.format(self.user_name)))
        df_divid.to_excel(os.path.join(self.output_folder, '{0}_divid.xlsx'.format(self.user_name)))
        self.df_raw_record_list = df_trade.copy()
        # 取指针
        self.df_divid = df_divid.copy()
        pass

    def deal_type_calc(self, x):
        op = x['businType']
        if op in ['买基金']:
            return '买入'
        elif op in ['卖基金','卖基金极速回活期宝','超级转换-转出','卖基金回活期宝']:
            return '卖出'
        elif op in ['强增']:
            return '分红'
        else:
            return op

    def deal_money_calc(self, x):
        money = float(x['cfmAmount'])
        fee = float(x['fee'])
        op = x['deal_type']
        if op == '买入':
            return round(money - fee, 2)
        elif op == '卖出':
            return round(money, 2)
        else:
            return money

    def occur_money_calc(self, x):
        money = float(x['cfmAmount'])
        fee = float(x['fee'])
        op = x['deal_type']
        if op == '买入':
            return round(money, 2)
        elif op == '卖出':
            return round(money - fee, 2)
        else:
            return money

    def get_records(self):
        # 读取逻辑过于复杂，自己补充的交易记录（转托管）
        df_addtion_record = None
        with open(self.input_file, 'r', encoding='utf-8') as f:
            df_addtion_record = pd.DataFrame([(pd.Series(x)) for x in json.loads(f.read())])
             
        # 分红信息
        # fundCode	fundName	dividendMethod	nav	dividendVol	dividendAmount	confirmDate
        # 100032	富国中证红利指数增强	红利再投资	1.2150	1085.17	--	2018-01-19
        self.df_divid = self.df_divid[~self.df_divid['fundName'].str.contains('货币')]
        self.df_divid = self.df_divid.replace(['--'],[0.0])
        self.df_divid = self.df_divid.reset_index(drop=True)
        self.df_divid['id'] = self.df_divid.index + 1
        self.df_divid['date'] = self.df_divid['confirmDate']
        self.df_divid['time'] = '14:59:00'
        self.df_divid['code'] = self.df_divid['fundCode']
        self.df_divid['code'] = self.df_divid['code'].apply(lambda x: str(x).zfill(6))
        self.df_divid['name'] = self.df_divid['fundName']
        self.df_divid['deal_type'] =  '分红'
        self.df_divid['nav_unit'] = self.df_divid['nav']
        self.df_divid['nav_acc'] = self.df_divid['nav']
        self.df_divid['volume'] = self.df_divid['dividendVol']
        self.df_divid['deal_money'] = self.df_divid['dividendAmount']
        self.df_divid['fee'] = 0.0
        self.df_divid['occur_money'] = self.df_divid['dividendAmount']
        self.df_divid['account'] = '{0}_天天'.format(self.user_name)
        self.df_divid['unique_id'] = self.df_divid['name'] + '_' + self.df_divid['date'] + '_' + self.df_divid['dividendMethod']
        self.df_divid['note'] = '无'
        # 补充三级分类
        self.df_divid = pd.merge(self.df_divid, self.cm.df_category, left_on='code', right_on='基金代码', how='left')
        self.df_divid = self.df_divid.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        self.df_divid['name'] = self.df_divid['基金名称']
        self.df_divid = self.df_divid[record_keys()]

        # 成交记录
        # fundCode	fundName	bankName	bankCardNo	businTypeId	businType	nav	cfmAmount	cfmVol	charge	cfmState	transactionCfmDate
        # 161725	白酒指数	招商银行	 6292	      124	      卖基金	0.8440	475.59	  566.33	2.39	成功	   2016-05-30
        df = self.df_raw_record_list.copy()
        # 去掉货币基金数据
        df = df[~df['fundName'].str.contains('货币')]
        # 非货币基金的操作有，businType：
        # ['卖基金', '买基金', '修改分红方式', '卖基金极速回活期宝', '强增', '跨分账户份额转卡', '超级转换-转出', '转托管']
        ignore_types = ['修改分红方式', '跨分账户份额转卡', '转托管']
        # 去掉一些和成本无关的操作（转托管记录有 addition.json 代劳）
        df = df[~df['businType'].isin(ignore_types)]
        df = df.reset_index(drop=True)
        df['id'] = df.index + 1
        df['date'] = df['transactionCfmDate']
        df['time'] = '14:59:00'
        df['code'] = df['fundCode']
        df['code'] = df['code'].apply(lambda x: str(x).zfill(6))
        df['name'] = df['fundName']
        df['deal_type'] =  df.apply(self.deal_type_calc, axis=1)
        df['nav_unit'] = df['nav']
        df['nav_acc'] = df['nav']
        df['volume'] = df['cfmVol']
        df['fee'] = df['charge']
        df['deal_money'] = df.apply(self.deal_money_calc, axis=1)
        df['occur_money'] = df.apply(self.occur_money_calc, axis=1)
        df['account'] = '{0}_天天'.format(self.user_name)
        df['unique_id'] = df['serialNo']
        df['note'] = '无'
        # 补充三级分类
        df = pd.merge(df, self.cm.df_category, left_on='code', right_on='基金代码', how='left')
        df = df.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        df['name'] = df['基金名称']
        df = df[record_keys()]
        # print(df.businType.unique())
        # df[df.businType == '卖基金回活期宝']
        all = [df, self.df_divid]
        # 自补部分
        if len(df_addtion_record) > 0:
            df_addtion_record = pd.merge(df_addtion_record, self.cm.df_category, left_on='code', right_on='基金代码', how='left')
            df_addtion_record = df_addtion_record.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
            df_addtion_record['name'] = df_addtion_record['基金名称']
            df_addtion_record = df_addtion_record[record_keys()]
            all.append(df_addtion_record)
        # 合并
        df_results = pd.concat(all, ignore_index=True)
        df_results = df_results.sort_values(['date','code','category_id', 'code'])
        df_results = df_results.reset_index(drop=True)
        df_results['id'] = df_results.index + 1
        self.df_results = df_results.copy()
        pass

    def adjust_dates(self):
        """
        对账单中的日期都是确认日，净值日期应该整体前移一天
        """
        # 交易日集合
        jq = jqadapter()
        series_all_days = jq.get_trade_day_info()
        # 买入确认日信息（指数型 T+1，QDII T+2）
        fi = fundinfo()
        df_operate_info = fi.get_fund_operate_info()
        df_record = self.df_results.copy()
        # debug
        # df_record = pd.read_excel('1.xlsx', index_col=0, dtype={'code':str})
        confirm_dates = df_record.date.tolist()
        target_codes = df_record.code.tolist()
        deal_types = df_record.deal_type.tolist()
        apply_dates = []
        for i, x in enumerate(confirm_dates):
            code = target_codes[i]
            day_to_confirm = int(df_operate_info[df_operate_info['基金代码'] == code].买入确认日.values[0])
            # print(code, x, deal_types[i], day_to_confirm, series_all_days[series_all_days < x][-day_to_confirm])
            apply_dates.append(series_all_days[series_all_days < x][-day_to_confirm])
        df_record['date'] = apply_dates
        df_record['code'] = df_record['code'].apply(lambda x: str(x).zfill(6))
        self.df_results = df_record.copy()
        # debug
        # df_record.to_excel('2.xlsx')
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

if __name__ == "__main__":
    tt = tiantian()
    # 设置用户
    tt.set_user_id('klq')
    tt.set_user_id('lsy')
    tt.get()
    # tt.adjust_dates()