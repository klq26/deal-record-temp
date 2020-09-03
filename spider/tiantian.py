# -*- coding: utf-8 -*-

import os
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

class tiantian(absspider):
    
    def __init__(self, uname=None):
        self.app_name = '天天'
        self.headers = {}
        self.name_mapping = {'klq': '康力泉', 'lsy': '李淑云'}
        self.set_user_by_mapping(uname)
        self.date_intervals = []
        # 采用抓取“我的对账单”来获取数据
        self.start_year = 1970
        self.start_month = 1
        # 我和老妈的不太一样
        self.server_domain = 'trade'
        # 不可或缺的参数，抓取 bi.aspx 三个接口时，也把这个改了吧。不知道是自动生成还是每用户一个
        self.qkt = u'6b152f0b691e4e0980423b6c77aad53e'
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        pass
    
    def set_user_by_mapping(self, uname = None):
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
            self.date_intervals = self.get_date_intervals()
            self.start_year = 2016
            self.start_month = 5
            self.server_domain = 'trade'
            self.qkt = u'6b152f0b691e4e0980423b6c77aad53e'
        elif s_name == 'lsy':
            self.headers = self.wm.get_tiantian_lsy()
            self.date_intervals = self.get_date_intervals(startYear = 2018, startMonth = 1, startDay = 1)
            self.start_year = 2018
            self.start_month = 1
            self.server_domain = 'trade7'
            self.qkt = u'a7162f88be2943cab9b2cee962ee28c7'
        self.input_file_name = '{0}_addition.json'.format(self.user_name)

    def get_date_intervals(self, startYear = 2016, startMonth = 5, startDay = 1, interval = 90):
        """
        根据今天日期和起始日期,生成多个 90 天时间间隔的二维数组
        """
        date_intervals = []
        # 起始日期为开户日
        startDate = datetime(year = startYear, month = startMonth, day = startDay)
        # 间隔 90 天
        interval_days = timedelta(days = interval)
        # 和起始日间隔 90 天后的日期
        endDate = startDate + interval_days
        # 终止日期为今天
        today = datetime.now()
        # 字符串格式化
        fmt = '%Y-%m-%d'
        while max(today, endDate) == today:
            # print(startDate.strftime(fmt), endDate.strftime(fmt))
            date_intervals.append({'startDate': startDate.strftime(fmt), 'endDate': endDate.strftime(fmt), 'duration': interval_days.days})
            startDate = endDate + timedelta(days=1)
            endDate = startDate + interval_days
        # 最后一条不足 90 天的数据
        startDate = endDate - interval_days
        endDate = today
        # print(startDate.strftime(fmt), endDate.strftime(fmt))
        date_intervals.append({'startDate': startDate.strftime(fmt), 'endDate': endDate.strftime(fmt), 'duration': (today - startDate).days})
        return date_intervals

    def grequests_logging(self, req, *args, **kwargs):
        print('正在请求：{0}'.format(req.url))
        pass

    def grequests_error_handler(self, req, exception):
        print("{0} 请求出错: {1}".format(req.url, exception))
        pass

    # 整合所有的交易列表，生成详情 url 集合
    def get_detail_urls_from_tradelist(self, html_text):
        detail_url_list = []
        detail_url_prefix = u'https://query.1234567.com.cn'
        soup = BeautifulSoup(html_text, 'lxml')
        results = soup.findAll(lambda e: e.name == 'a' and '详情' in e.text)
        # 接口拿回来默认是时间倒叙的
        results = list(reversed(results))
        [detail_url_list.append(detail_url_prefix + x['href']) for x in results]
        return detail_url_list

    # 获取详细交易数据的整合信息
    def get_detail_info(self, html_text, url):
        soup = BeautifulSoup(html_text, 'lxml')
        # 从 ui-detail 中获取申请详细时间
        # 注意，分红信息是没有申请详细模块的，所以直接给 14:59:00
        ui_details = soup.findAll('div',{'class':'ui-detail'})
        if len(ui_details) > 0:
            applyTime = ui_details[0].select('td')[1].text.split(' ')[1]
            # 如果时间大于 15:00:00，如：18:39:44，则申请所在交易日应该是下一日
            # 但是天天基金给出的 applyDate 已经自动切换到申购交易日了，所以这些统一改成 14:59:00
            if int(applyTime[0:2]) >= 15:
                applyTime = '14:59:00'
        else:
            applyTime = '14:59:00'
            
        # 取申请、确认区
        results = soup.findAll('div',{'class':'ui-confirm'})
        applyInfo_keys = ['applyDate', 'applyOperate','applyStatus']
        confirmInfo_keys = ['fundName', 'fundCode', 'confirmDate', 'confirmOperate', 'confirmStatus', 'confirmNavUnit', 'confirmNavAcc', 'confirmMoney', 'confirmVolume', 'fee']

        jsonData = {'申请信息': {}, '确认信息': {}, '详情页': url }
        for x in results:
            h3_array = x.select('h3')
            # 结果
            values = []
            if len(h3_array) > 0 and h3_array[0].text == u'申请信息':
                if len(x.select('table')) > 0:
                    # 所有值
                    tags = x.select('table')[0].tbody.findAll('td')
                    # 不要“申请数额”
                    tags.pop(-2)
                    # 不要“标题<br/>代码”
                    tags.pop(1)
                    [values.append(x.text) for x in tags]
                    jsonData['申请信息'] = (dict(zip(applyInfo_keys, values)))
                    jsonData['申请信息']['applyTime'] = applyTime
            if len(h3_array) > 0 and h3_array[0].text == u'确认信息':
                if len(x.select('table')) > 0:
                    # 所有值
                    tags = x.select('table')[0].tbody.findAll('td')
                    # “标题<br/>代码”
                    a = tags.pop(1).a
                    # 标题
                    values.append(a.contents[0])
                    # 代码
                    values.append(a.contents[2])
                    [values.append(x.text) for x in tags]
                    # 拿一下累计净值
                    values.insert(6, '待实现')
                    jsonData['确认信息'] = (dict(zip(confirmInfo_keys, values)))
        return jsonData

    def get_trade_list(self):
        # qkt=6b152f0b691e4e0980423b6c77aad53e 不可或缺，抓 cookie 的时候搜索 bi.aspx
        # 持仓
        url_hold = u'https://{0}.1234567.com.cn/SearchHandler/bi.aspx?callback=callback&type=billhold&qkt={1}&ttype=month&year={2}&data={3}'
        # 交易
        url_trade = u'https://{0}.1234567.com.cn/SearchHandler/bi.aspx?callback=callback&type=billtrade&qkt={1}&ttype=month&year={2}&data={3}'
        # 分红
        url_divid = u'https://{0}.1234567.com.cn/SearchHandler/bi.aspx?callback=callback&type=billdivid&qkt={1}&ttype=month&year={2}&data={3}'

        ts_range = pd.date_range(start='{0}-{1}-01'.format(self.start_year, self.start_month), end=datetime.today().strftime('%Y-%m-%d'), freq='1M').tolist()
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
            tasks = [grequests.get(x, headers = self.headers) for x in urls]
            resp_list = grequests.map(tasks)
            for resp in resp_list:
                if u'billhold' in resp.request.url:
                    data_list = json.loads(resp.text.replace(');','').replace('callback(',''))['result']['datas']
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
        pass
    
    def get_raw_record_list(self):
        # 请求执行回调
        trade_list_url = u'https://query.1234567.com.cn/Query/DelegateList?DataType=1&StartDate={0}&EndDate={1}&BusType=0&Statu=0&Account=&FundType=0&PageSize=500&PageIndex=1'
        urls = []
        # [print(x) for x in urls]
        for interval in self.date_intervals:
            urls.append(trade_list_url.format(interval['startDate'], interval['endDate']))
        tasks = [grequests.get(x, headers=self.headers, callback=self.grequests_logging) for x in urls]
        # 控制一下并发量，天天服务器没那么抗造
        response_list = grequests.map(tasks, exception_handler=self.grequests_error_handler, size=2)
        [print(x.request.url, x.status_code, x.text.strip()[0:30]) for x in response_list]

        # 遍历 trade_list
        series_list = []
        i = 1
        for response in response_list:
            date_interval_urls = self.get_detail_urls_from_tradelist(response.text)
            print('{0}/{1} total:{2}'.format(i, len(response_list), len(date_interval_urls)))
            i += 1
            tasks = [grequests.get(x, headers=self.headers) for x in date_interval_urls]
            # 控制一下并发量，天天服务器没那么抗造
            record_response_list = grequests.map(tasks, exception_handler=self.grequests_error_handler, size=2)
            for response in record_response_list:
                jsonData = self.get_detail_info(response.text, response.request.url)
        #         print(jsonData)
        #         print(response.request.url)
                item = dict()
                if len(jsonData['申请信息'].keys()) > 0:
                    for k, v in jsonData['申请信息'].items():
                        item[k] = v
                if len(jsonData['确认信息'].keys()) > 0:
                    for k, v in jsonData['确认信息'].items():
                        item[k] = v
                item['详情页'] = jsonData['详情页']
                series_list.append(pd.Series(item))
        #     break
        df = pd.DataFrame(series_list)
        df = df[['applyDate', 'applyTime', 'applyOperate', 'applyStatus', 'fundName', 'fundCode', 'confirmDate', 'confirmOperate', 'confirmStatus', 'confirmNavUnit', 'confirmNavAcc', 'confirmMoney', 'confirmVolume', 'fee', '详情页']]
        self.df_raw_record_list = df.copy()
        pass

    def date_calc(self, x):
        if str(x['applyDate']) == 'nan':
            return x['confirmDate']
        else:
            return x['applyDate']

    def time_calc(self, x):
        if str(x['applyTime']) == 'nan':
            return '14:59:00'
        else:
            return x['applyTime']

    def deal_type_calc(self, x):
        op = x
        if '申购' in op:
            return '买入'
        elif '赎回' in op:
            return '卖出'
        elif '红利' in op or '强行调' in op:
            return '分红'
        else:
            return x

    def deal_money_calc(self, x):
        op = x['deal_type']
        money = float(x['confirmMoney'])
        fee = float(x['fee'])
        deal_money = 0.0
        if op == '买入':
            deal_money = round(money - fee, 2)
        elif op == '卖出':
            deal_money = money
        elif op == '分红':
            deal_money = money
        return deal_money

    def occur_money_calc(self, x):
            op = x['deal_type']
            money = float(x['confirmMoney'])
            fee = float(x['fee'])
            occur_money = 0.0
            if op == '买入':
                occur_money = money
            elif op == '卖出':
                occur_money = round(money - fee, 2)
            elif op == '分红':
                occur_money = money
            return occur_money

    def get_records(self):
        # 读取逻辑过于复杂，自己补充的交易记录（分红、转换）
        df_addtion_record = None
        with open(self.input_file, 'r', encoding='utf-8') as f:
            df_addtion_record = pd.DataFrame([(pd.Series(x)) for x in json.loads(f.read())])
        
        # 	applyDate	applyTime	applyOperate	applyStatus	fundName	fundCode
        # confirmDate	confirmOperate	confirmStatus	confirmNavUnit	confirmNavAcc	confirmMoney	confirmVolume	fee	

        # applyOperate: '买入申请', nan, '充值申请', '赎回申请', '快速过户'
        # applyStatus: '已受理(支付完成)', nan, '受理失败(支付失败)', '已受理(无需支付)', '已撤单(已支付)'
        # confirmOperate: '申购确认', '赎回确认', '红利发放(红利再投资)', '转出投资账户确认', '转入投资账户确认', '转托管确认',
        #                 '红利发放(现金分红)','强行调增'

        # '转出投资账户确认', '转入投资账户确认' 是无意义的，属于网站行为

        df = self.df_raw_record_list.copy()
        # 去掉已撤单 or 受理失败导致的记录，这是 fundName 为空仅有的合理解释
        df = df[~(df['fundName'].isnull())]
        # 货币基金咱也不要了
        df = df[~df['fundName'].str.contains('货币')]
        # '转出投资账户确认' & '转入投资账户确认' 无交易，只是组合操作，忽略, '转托管确认' 在 addition.json 中，此处忽略
        df = df[~df['confirmOperate'].isin(['转出投资账户确认', '转入投资账户确认', '转托管确认'])]
        df['id'] = df.reset_index().index + 1
        df['date'] = df.apply(self.date_calc, axis=1)
        df['time'] = df.apply(self.time_calc, axis=1)
        df['code'] = df['fundCode'].apply(lambda x: str(int(x)).zfill(6))
        df['name'] = df['fundName']
        df['deal_type'] = df['confirmOperate'].apply(self.deal_type_calc)
        df['volume'] = df['confirmVolume']
        # df['fee'] = df['fee']
        df['nav_unit'] = df['confirmNavUnit']
        df['nav_acc'] = df['confirmNavUnit']
        df['deal_money'] = df.apply(self.deal_money_calc, axis=1)
        df['occur_money'] = df.apply(self.occur_money_calc, axis=1)
        df['account'] = '{0}_天天'.format(self.user_name)
        # # 补充一二三级分类
        df = pd.merge(df, self.cm.df_category, left_on='code', right_on='基金代码', how='left')
        df = df.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        df['unique_id'] = df['applyDate'] + '_' + df['applyTime'] + '_' + df['code']
        df['note'] = df['confirmOperate'] + '_' + df['详情页']
        # 补充额外数据
        if len(df_addtion_record) > 0:
            df = pd.concat([df, df_addtion_record])
        df = df.reset_index(drop=True)
        df['id'] = df.index + 1
        df = df.sort_values(['date','time','code'])
        df = df[record_keys()]
        self.df_results = df.copy()
        pass

if __name__ == "__main__":
    tt = tiantian()
    # 设置用户
    tt.set_user_by_mapping('klq')
    # tt.get()
    tt.get_trade_list()