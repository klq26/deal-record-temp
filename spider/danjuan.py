# -*- coding: utf-8 -*-

import json
import time, datetime

import grequests
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import numpy as np

from absspider import absspider
from record import record_keys

class danjuan(absspider):

    def __init__(self, uname = None):
        self.app_name = '蛋卷'
        self.headers = {}
        self.name_mapping = {'klq': '康力泉', 'lsy': '李淑云', 'ksh': '康世海'}
        self.set_user_id(uname)
        # 蛋卷的交易里面有组合有单独基金，两种情况，解析方法不一样，下面是暂存单独基金交易数据列表的，属于蛋卷特有的情况
        self.df_fund_output_list = pd.DataFrame()
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        pass
    
    def set_user_id(self, uname = None):
        """
        判断用户是谁，应该读取哪个配置文件
        """
        s_name = ''
        if uname and uname in ['klq', 'lsy', 'ksh']:
            s_name = uname
        else:
            s_name = 'klq'
        self.user_name = self.name_mapping[s_name]
        if s_name == 'klq':
            self.headers = self.wm.get_danjuan_klq()
        elif s_name == 'lsy':
            self.headers = self.wm.get_danjuan_lsy()
        elif s_name == 'ksh':
            self.headers = self.wm.get_danjuan_ksh()
        pass

    def get_trade_list(self):
        # 获取交易列表
        trade_list_url = u'https://danjuanapp.com/djapi/order/p/list?page=1&size=2000&type=all'
        response = requests.get(trade_list_url, headers=self.headers, verify=False)
        trade_list = response.json()['data']['items']
        # [print(x) for x in trade_list]
        # 并发获取所有的交易列表详情
        series_trade_list = [pd.Series(x) for x in trade_list]
        df = pd.DataFrame(series_trade_list)
        # 去掉失败、撤单、部分交易成功、交易进行中的交易记录单
        df = df[~(df['status_desc'].isin(['交易失败', '已撤单', '部分交易成功','交易进行中']))]
        # created_at 换成可读形式 1598332156752
        df['created_at'] = df.created_at.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df['created_at'] = df['created_at'].astype(np.datetime64)
        df = df.sort_values(['created_at'])
        df = df.reset_index(drop=True)
        df.index.name = 'id'
        # ['CSI666', 'CSI1021', 'CSI1019', '003474']
        # action ['022', '024', '143', '036']
        # 唯一标识：order_id，之后用来做 increment 增量更新
        self.df_trade_list = df.copy()
        pass
    
    def get_raw_record_list(self):
        # 取回所有的成交记录
        trade_detail_url = u'https://danjuanapp.com/djapi/order/p/plan/{0}'
        # trade_list 唯一 id 集合
        order_id_list = self.df_trade_list.order_id.tolist()
        tasks = [grequests.get(u'https://danjuanapp.com/djapi/order/p/plan/{0}'.format(x), headers=self.headers) for x in order_id_list]
        response_list = grequests.map(tasks)
        # 汇总所有的成交记录

        # 组合交易
        plan_record_list = []
        # 单独基金交易
        fund_record_list = []

        for i in range(len(response_list)):
            response = response_list[i]
            if response.status_code != 200:
                print('[Error]: {0} 请求失败'.format(response.request.url))
                exit(1)
            records_data = response.json()['data']
            # 区别对待 单独买卖基金 和 买卖组合
            if 'sub_order_list' not in records_data.keys():
                fund_record_list.append(pd.Series(response.json()['data']))
            else:
                plan_record_list.append(pd.Series(response.json()['data']))

        df_plan_record_list = pd.DataFrame(plan_record_list)
        df_fund_record_list = pd.DataFrame(fund_record_list)

        # 单独基金交易
        df_fund_record_list['created_at'] = df_fund_record_list.created_at.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df_fund_record_list['created_at'] = df_fund_record_list['created_at'].astype(np.datetime64)
        df_fund_record_list = df_fund_record_list.sort_values(['created_at'])
        # df_fund_record_list.to_excel('02_{0}_fund_order_list.xlsx'.format(self.user_name), sheet_name=f'{self.user_name}_交易记录')
        # 屏蔽货币基金
        df_fund_record_list = df_fund_record_list[~df_fund_record_list.name.str.contains('货币')]
        if len(df_fund_record_list) > 0:
            df_fund_output_list = self.handle_fund_trades(df_fund_record_list['order_id'].tolist())
        else:
            print('没有单独基金交易')
            df_fund_output_list = pd.DataFrame()
        # 取得单独基金交易的指针
        self.df_fund_output_list = df_fund_output_list.copy()


        # 组合操作
        df_plan_record_list['created_at'] = df_plan_record_list.created_at.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df_plan_record_list['created_at'] = df_plan_record_list['created_at'].astype(np.datetime64)
        df_plan_record_list['final_confirm_date'] = df_plan_record_list.final_confirm_date.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df_plan_record_list['final_confirm_date'] = df_plan_record_list['final_confirm_date'].astype(np.datetime64)
        df_plan_record_list = df_plan_record_list.sort_values(['created_at','final_confirm_date'])
        # df_plan_record_list 比 df_trade_list 多了下面 6 列
        # ['final_confirm_date', 'total_confirm_amount', 'total_fee', 'bank_name', 'sub_order_list', 'total_confirm_volume']
        # 其中，sub_order_list 是详细数据。可能包含 1 ~ n 个对象。每个对象下的 orders 就是具体成交记录
        # df_plan_record_list.to_excel('02_{0}_plan_order_list.xlsx'.format(self.user_name), sheet_name=f'{self.user_name}_交易记录')
        # df_plan_record_list[['final_confirm_date', 'total_confirm_amount', 'total_fee', 'bank_name', 'sub_order_list', 'total_confirm_volume']]

        # 每一条成交记录原始表
        record_lists = []
        for sub_order in df_plan_record_list['sub_order_list']:
            # 如果是转换操作，则 sub_order 就有两个对象
            for i in range(len(sub_order)):
                # 每一个 sub_order 下，每只基金的真实成交列表 orders
                orders = sub_order[i]['orders']
                for record in orders:
                    record_lists.append(pd.Series(record))

        # 生成最原始的成交记录列表
        df_raw = pd.DataFrame(record_lists)
        df_raw.index.name = 'id'
        df_raw['ts'] = df_raw.ts.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df_raw['ts'] = df_raw['ts'].astype(np.datetime64)
        df_raw['confirm_ts'] = df_raw.confirm_ts.apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        df_raw['confirm_ts'] = df_raw['confirm_ts'].astype(np.datetime64)
        df_raw = df_raw.sort_values(['ts','confirm_ts'])
        self.df_raw_record_list = df_raw.copy()
        pass

    def get_records(self):
        # 产出清洗过后的数据
        # ['买入', '卖出', '分红', '转换', '组合转出', '组合转入']
        # df_raw.action_desc.unique()
        df_raw = self.df_raw_record_list.copy()
        # 货币基金转换
        df_money_fund_exchange = df_raw[df_raw.action_desc.isin(['转换'])]
        # 投资组合内部转换
        df_plan_exchange = df_raw[df_raw.action_desc.isin(['组合转出', '组合转入'])]
        # 正常买入卖出交易
        df_normal = df_raw[df_raw.action_desc.isin(['买入', '卖出', '分红'])]
        
        # 调整输出

        # 买入、卖出、分红
        df_temp = df_normal.copy()
        # print(len(df_temp))
        df_temp['id'] = df_normal.reset_index().index + 1
        df_temp['date'] = df_temp['ts'].apply(lambda x: str(x)[0:10])
        df_temp['time'] = '14:59:00'
        df_temp['code'] = df_temp['fd_code']
        df_temp['name'] = df_temp['fd_name']
        df_temp['deal_type'] = df_temp['action_desc']
        df_temp['volume'] = df_temp['confirm_volume']
        df_temp['fee'] = df_temp['fee']
        df_temp['nav_unit'] = df_temp.apply(self.nav_unit_calc, axis=1)
        df_temp['nav_acc'] = df_temp['nav_unit']
        df_temp['deal_money'] = df_temp['confirm_amount']
        df_temp['occur_money'] = df_temp.apply(self.occur_money_calc, axis=1)
        df_temp['account'] = '{0}_蛋卷'.format(self.user_name)
        # # 补充一二三级分类
        df_temp = pd.merge(df_temp, self.cm.df_category, left_on='fd_code', right_on='基金代码', how='left')
        df_temp = df_temp.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        df_temp['unique_id'] = df_temp['order_id']
        df_temp['note'] = df_temp['plan_name'] + '_' + df_temp['deal_type'] + '_orderid=' + df_temp['order_id']

        df_temp = df_temp[record_keys()]
        # df_temp.to_excel('04_{0}_normal_record_list.xlsx'.format(self.user_name), sheet_name=f'{self.user_name}_交易记录')

        # 货币基金转换
        df_money_temp = df_money_fund_exchange.copy()
        # print(len(df_money_temp))
        if len(df_money_temp) > 0:
            df_money_temp['id'] = df_money_temp.reset_index().index + 1
            df_money_temp['date'] = df_money_temp['ts'].apply(lambda x: str(x)[0:10])
            df_money_temp['time'] = '15:00:00'
            df_money_temp['code'] = df_money_temp['target_fd_code']
            df_money_temp['name'] = df_money_temp['target_fd_name']
            # 目前的基金转换，我只做过南方天天利货币B转指数基金的操作，以后将尽量避免这种繁琐的交易记录
            df_money_temp['action_desc'] = '买入'
            df_money_temp['deal_type'] = df_money_temp['action_desc']
            df_money_temp['volume'] = np.abs(df_money_temp['confirm_volume'])
            df_money_temp['fee'] = df_money_temp['fee']
            df_money_temp['nav_unit'] = df_money_temp.apply(self.nav_unit_calc, axis=1)
            df_money_temp['nav_acc'] = df_money_temp['nav_unit']
            df_money_temp['deal_money'] = df_money_temp['confirm_amount']
            df_money_temp['occur_money'] = df_money_temp.apply(self.occur_money_calc, axis=1)
            df_money_temp['account'] = '{0}_蛋卷'.format(self.user_name)
            # # 补充一二三级分类
            df_money_temp = pd.merge(df_money_temp, self.cm.df_category, left_on='target_fd_code', right_on='基金代码', how='left')
            df_money_temp = df_money_temp.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
            df_money_temp['unique_id'] = df_money_temp['order_id']
            df_money_temp['note'] = df_money_temp['fd_name'] + '_转换_' + df_money_temp['target_fd_name'] + '_orderid=' + df_money_temp['order_id']
            df_money_temp = df_money_temp[record_keys()]

        # 组合内部转换
        df_plan_exchange_temp = df_plan_exchange.copy()
        # print(len(df_plan_exchange_temp))
        df_plan_exchange_temp['id'] = df_plan_exchange_temp.reset_index().index + 1
        df_plan_exchange_temp['date'] = df_plan_exchange_temp['ts'].apply(lambda x: str(x)[0:10])
        df_plan_exchange_temp['time'] = '15:00:00'
        df_plan_exchange_temp['code'] = df_plan_exchange_temp['fd_code']
        df_plan_exchange_temp['name'] = df_plan_exchange_temp['fd_name']
        # 目前的基金转换，我只做过南方天天利货币B转指数基金的操作，以后将尽量避免这种繁琐的交易记录
        df_plan_exchange_temp['action_desc_exchange'] = df_plan_exchange_temp['action_desc']
        df_plan_exchange_temp['action_desc'] = df_plan_exchange_temp.apply(self.deal_type_calc, axis=1)
        df_plan_exchange_temp['deal_type'] = df_plan_exchange_temp['action_desc']
        df_plan_exchange_temp['volume'] = np.abs(df_plan_exchange_temp['confirm_volume'])
        df_plan_exchange_temp['fee'] = df_plan_exchange_temp['fee']
        df_plan_exchange_temp['nav_unit'] = df_plan_exchange_temp.apply(self.nav_unit_calc, axis=1)
        df_plan_exchange_temp['nav_acc'] = df_plan_exchange_temp['nav_unit']
        df_plan_exchange_temp['deal_money'] = df_plan_exchange_temp['confirm_amount']
        df_plan_exchange_temp['occur_money'] = df_plan_exchange_temp.apply(self.occur_money_calc, axis=1)
        df_plan_exchange_temp['account'] = '{0}_蛋卷'.format(self.user_name)
        # # 补充一二三级分类
        df_plan_exchange_temp = pd.merge(df_plan_exchange_temp, self.cm.df_category, left_on='fd_code', right_on='基金代码', how='left')
        df_plan_exchange_temp = df_plan_exchange_temp.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        df_plan_exchange_temp['unique_id'] = df_plan_exchange_temp['order_id']
        df_plan_exchange_temp['note'] = df_plan_exchange_temp['plan_name'] + '_' + df_plan_exchange_temp['action_desc_exchange'] + '_' + df_plan_exchange_temp['target_fd_name'] +  '_orderid=' + df_plan_exchange_temp['order_id']
        df_plan_exchange_temp = df_plan_exchange_temp[record_keys()]

        # 最终合并
        all = []
        if len(df_temp) > 0:
            all.append(df_temp)
        if len(df_money_temp) > 0:
            all.append(df_money_temp)
        if len(df_plan_exchange_temp) > 0:
            all.append(df_plan_exchange_temp)
        # 如果有基金交易，那也算上吧
        if len(self.df_fund_output_list) > 0:
            all.append(self.df_fund_output_list)
        df_output = pd.concat(all,ignore_index=True, sort=False)
        df_output = df_output.sort_values(['date', 'code'])
        df_output = df_output.reset_index(drop=True)
        df_output.id = df_output.index + 1
        self.df_results = df_output.copy()
        # df_output.to_excel('04_{0}_蛋卷.xlsx'.format(self.user_name), sheet_name=f'{self.user_name}_交易记录')
        pass

    def handle_fund_trades(self, order_ids):
        """
        处理单只基金的买入卖出操作
        """
        # 单只基金无法在 order 页面查看手续费，需要进一步请求
        # 并发
        fund_trade_url = 'https://danjuanapp.com/djapi/fund/order/{0}'
        tasks = [grequests.get(fund_trade_url.format(x), headers=self.headers) for x in order_ids]
        response_list = grequests.map(tasks)
        results = []
        i = 1
        for response in response_list:
            order = response.json()['data']
            s = dict()
            s['id'] = i
            # TODO 这里要判断 15 点前还是 15 点后
            s['date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(order['created_at'])/1000))[0:10]
            s['time'] = '14:59:00'
            s['code'] = order['fd_code']
            s['name'] = order['fd_name']
            s['deal_type'] = order['action_desc']
            s['volume'] = order['confirm_volume']
            s['deal_money'] = order['confirm_amount']
            confirm_infos = order['confirm_infos']
            fee = 0.0
            nav_unit = 0.0
            if len(confirm_infos) > 0:
                infos = confirm_infos[0]
                if len(infos) > 0:
                    for i in range(len(infos)-1, 0, -1):
                        info = infos[i]
                        if u'手续费' in info:
                            # 手续费,0.04元
                            feeStr = info.replace('手续费,','').replace('元','')
                            fee = round(float(feeStr),2)
                        if u'确认净值' in info:
                            navStr = info.replace('确认净值,','').replace('元','')
                            nav_unit = round(float(navStr),4)
            s['nav_unit'] = nav_unit
            s['nav_acc'] = nav_unit
            s['fee'] = fee
            opType = s['deal_type']
            if opType == '买入' or opType == '转换':
                occur_money = round(s['deal_money'] + fee, 2)
            elif opType == '卖出':
                occur_money = round(s['deal_money'] - fee, 2)
            elif opType == '分红':
                occur_money = round(s['deal_money'], 2)
            s['occur_money'] = occur_money
            s['account'] = '{0}_蛋卷'.format(self.user_name)
            s['unique_id'] = order['order_id']
            # s['note'] = 螺丝钉指数基金组合_买入_orderid=2198590579952573614
            s['note'] = order['fd_name'] + '_' + order['action_desc'] + '_orderid=' + order['order_id']
            results.append(pd.Series(s))
            i += 1
        df_fund_trades = pd.DataFrame(results)
        # 补充三级分类
        df_fund_trades = pd.merge(df_fund_trades, self.cm.df_category, left_on='code', right_on='基金代码', how='left')
        df_fund_trades = df_fund_trades.rename(columns={'一级分类': 'category1', '二级分类': 'category2', '三级分类': 'category3', '分类ID': 'category_id'})
        df_fund_trades = df_fund_trades[record_keys()]
        return df_fund_trades

    def nav_unit_calc(self, x):
        # 后面会根据数据库和 confirm_ts 进行修改的，这里只是暂存
        volume = x['confirm_volume']
        money = x['confirm_amount']
        fee = x['fee']
        nav_unit = 1.0
        if x['action_desc'] == '买入':
            nav_unit = round((money - fee) / volume, 4)
        elif x['action_desc'] == '卖出':
            nav_unit = round((money + fee) / volume, 4)
        elif x['action_desc'] == '分红':
            nav_unit = 1.0
        return nav_unit

    def deal_type_calc(self, x):
        if x['action_desc'] == '组合转入':
            return '买入'
        elif x['action_desc'] == '组合转出':
            return '卖出'
        else:
            return x['action_desc']

    def occur_money_calc(self, x):
        # 后面会根据数据库和 confirm_ts 进行修改的，这里只是暂存
        money = x['confirm_amount']
        fee = x['fee']
        occur_money = 0.0
        if x['action_desc'] in ['买入', '转换']:
            occur_money = round(money + fee, 4)
        elif x['action_desc'] == '卖出':
            occur_money = round(money - fee, 4)
        elif x['action_desc'] == '分红':
            occur_money = money
        return occur_money

if __name__ == "__main__":
    dj = danjuan()
    # 设置用户
    dj.set_user_id('lsy')
    dj.get()