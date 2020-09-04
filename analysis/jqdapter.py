import os
import sys
from os import path
import json
from datetime import datetime
from datetime import timedelta

import pandas as pd
import numpy as np
from jqdatasdk import *

# 加根目录到 sys.path
project_folder = os.path.abspath(os.path.join(path.dirname(__file__), os.pardir))
if project_folder not in sys.path:
    sys.path.append(project_folder)

from login.account import account
from category.categories import categories
from recordloader import recordloader

pd.set_option('display.max_columns', 30)
pd.set_option('display.max_rows', 1100)

class jqdapter:

    def __init__(self):
        # 分类信息
        self.cm = categories()
        # 登录聚宽
        a = account()
        auth(a.joinquant_user, a.joinquant_password)
        self.rc_loader = recordloader()
        # 获取所有人买过的所有基金的基金唯一代码（去重）
        self.unique_fund_codes = self.get_unique_fund_codes()
        pass
    
    def get_nav_info(self):
        """
        获取历史上所有基金的非分红交易（买入、卖出等）
        TODO 转托管这里好像，不能裸替了事
        """

        # 说一下逻辑：
        # 1. 从全家所有成交记录中，取出每一只基金的全部记录，并按交易日期去重
        # 2. 因为转托管不是真实价格，所以忽略，分红需要除权日数据，也暂时忽略

        df_results = pd.DataFrame()
        # 所有记录
        df = self.get_all_users_combine_records()
        for code_index, code in enumerate(self.unique_fund_codes):
            # 忽略分红和转托管
            sub_df = df[(df.code == code) & ~(df.deal_type == '分红') & ~(df.deal_type == '托管转出') & ~(df.deal_type == '托管转入')]
            fund_trade_days = sub_df.date.unique().tolist()
            # 开始日期
            start_date = fund_trade_days[0]
            start_year = int(start_date[0:4])
            start_month = int(start_date[5:7])
            start_day = int(start_date[8:10])
            # 结束日期
            end_date = datetime.now().strftime('%Y-%m-%d')
            end_year = int(end_date[0:4])
            end_month = int(end_date[5:7])
            # 把开始和结束日期之间，每个离月末最近的交易日期取出来
            date_range = []
            for year in range(start_year, end_year + 1):
                if year == start_year:
                    [date_range.append('{0}-{1}-01'.format(year, str(x).zfill(2))) for x in range(start_month, 13)]
                    # 去掉第一个月，因为起始月交易，至少要到月底才能计算月收益
                    date_range.pop(0)
                elif year > start_year and year < end_year:
                    [date_range.append('{0}-{1}-01'.format(year, str(x).zfill(2))) for x in range(1, 13)]
                elif year == end_year:
                    [date_range.append('{0}-{1}-01'.format(year, str(x).zfill(2))) for x in range(1, end_month + 1)]
            # 从聚宽取的，每个月最后一个有效交易日
            jq_eof_month_trade_days = []
            for i, day in enumerate(date_range):
                end_date = datetime.strptime(day, '%Y-%m-%d') - timedelta(days=1)
                # 以月末为 end_date 向前去一天交易日数据（可以是月末这一天，也可以是之前的非休息日）
                jq_eof_month_trade_days.append(get_trade_days(end_date=end_date, count=1).tolist()[0].strftime('%Y-%m-%d'))
            # print('成交日', fund_trade_days)
            # print('月末日', jq_eof_month_trade_days)
            dates = []
            # 合并去重
            [dates.append(x) for x in fund_trade_days]
            [dates.append(x) for x in jq_eof_month_trade_days]
            series = pd.Series(dates)
            # 去重后的基金代码
            unique_nav_days = series.unique()
            print('获取 {0} 净值。进度 {1} / {2}，共计 {3} 天'.format(code, code_index + 1, len(self.unique_fund_codes), len(unique_nav_days)))
            # 取净值
            # 查询净值（交易日（买入、卖出、分红），月末净值（统计用））
            nav_query = query(finance.FUND_NET_VALUE).filter(
                finance.FUND_NET_VALUE.code == code,
                finance.FUND_NET_VALUE.day.in_(unique_nav_days)
            ).order_by(
                finance.FUND_NET_VALUE.day.asc()
            )
            df_jq_fund_nav = finance.run_query(nav_query)
            # id 就是个渣渣
            df_jq_fund_nav = df_jq_fund_nav.drop(['id'], axis=1)
            cn_cols = {
                'code': '基金代码',
                'day': '交易日',
                'net_value': '单位净值',
                'sum_value': '累计净值',
                'factor': '复权因子',
                'acc_factor': '累计复权因子',
                'refactor_net_value': '累计复权净值'
            }
            df_jq_fund_nav = df_jq_fund_nav.rename(columns=cn_cols)
            df_results = pd.concat([df_results, df_jq_fund_nav], ignore_index=True)
            # print(df_jq_fund_nav)
        df_results.to_excel('基金净值数据.xlsx')
        pass

    def get_divid_info(self):
        """
        获取历史上所有交易过的基金的分红、拆分数据
        """
        # 分红、拆分、合并
        dividend_split_query = query(finance.FUND_DIVIDEND).filter(
                finance.FUND_DIVIDEND.code.in_(self.unique_fund_codes)
            ).order_by(
                finance.FUND_DIVIDEND.code.asc(),
                finance.FUND_DIVIDEND.pub_date.asc(),
            )
        df_jq_dividend_split = finance.run_query(dividend_split_query)
        # 中文字段名
        cn_columns = {
            'code': '基金代码',
            # 'name': '基金名称',
            'pub_date': '公布日期',
            'event_id': '事项类别',
            'event': '事项名称',
            'distribution_date': '分配收益日',
            'process_id': '方案进度编码',
            'process': '方案进度',
            'proportion': '派现比例',
            'split_ratio': '分拆（合并、赠送）比例',
            'record_date': '权益登记日',
            'ex_date': '除息日',
            'fund_paid_date': '基金红利派发日',
            'redeem_date': '再投资赎回起始日',
            'dividend_implement_date': '分红实施公告日',
            'dividend_cancel_date': '取消分红公告日',
            'otc_ex_date': '场外除息日',
            'pay_date': '红利派发日',
            'new_share_code': '新增份额基金代码',
            'new_share_name': '	新增份额基金名称'
        }
        # df_jq_dividend_split = pd.read_excel('test.xlsx', index_col=0, dtype={'code': str})
        # 无用 id
        df_jq_dividend_split = df_jq_dividend_split.drop(['id'], axis=1)
        df_jq_dividend_split = df_jq_dividend_split.rename(columns=cn_columns)
        origin_columns = df_jq_dividend_split.columns
        df_jq_dividend_split = pd.merge(df_jq_dividend_split, self.cm.df_category, left_on='基金代码', right_on='基金代码', how='left')
        df_jq_dividend_split['name'] = df_jq_dividend_split['基金名称']
        df_jq_dividend_split = df_jq_dividend_split[origin_columns]
        df_jq_dividend_split = df_jq_dividend_split.rename(columns={'name': '基金名称'})
        df_jq_dividend_split.to_excel('分红拆分记录.xlsx')
        pass

    def get_unique_fund_codes(self):
        """
        获取所有人买过的所有基金的基金唯一代码（去重）
        """
        fund_codes = []
        # 取康力泉所有成交记录基金唯一代码
        self.rc_loader.set_user_id('klq')
        [fund_codes.append(x) for x in self.rc_loader.get_all_fund_unique_codes()]
        # 取父母所有成交记录基金唯一代码
        self.rc_loader.set_user_id('parents')
        [fund_codes.append(x) for x in self.rc_loader.get_all_fund_unique_codes()]
        series = pd.Series(fund_codes)
        # 去重后的基金代码
        fund_codes = series.unique()
        return fund_codes

    def get_all_users_combine_records(self):
        """
        获取全部用户的所有交易记录，放入一张表，用来取基金净值
        """
        # 获取康力泉所有记录
        self.rc_loader.set_user_id('klq')
        df1 = self.rc_loader.get_records()
        # 获取父母所有记录
        self.rc_loader.set_user_id('parents')
        df2 = self.rc_loader.get_records()
        df = pd.concat([df1, df2], ignore_index=True)
        df = df.sort_values(['date','time','category_id','code'])
        df = df.reset_index(drop=True)
        # 家庭整体交易记录
        df.id = df.index + 1
        return df

if __name__ == "__main__":
    jq = jqdapter()
    jq.get_nav_info()