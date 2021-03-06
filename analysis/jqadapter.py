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
from analysis.recordloader import recordloader

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class jqadapter:

    def __init__(self):
        self.folder = os.path.abspath(os.path.dirname(__file__))
        # 分类信息
        self.cm = categories()
        # 登录聚宽
        a = account()
        auth(a.joinquant_user, a.joinquant_password)
        self.rc_loader = recordloader()
        # 获取所有人买过的所有基金的基金唯一代码（去重）
        self.unique_fund_codes = self.get_unique_fund_codes()
        pass
    
    def get_trade_day_info(self):
        """
        获取全部交易日（注意，一年的第一天就能知道今年所有的交易日，所以 2020-01-01 就能拿到 2020 年底的数据）
        """
        file_path = path.join(self.folder, u'全部交易日.xlsx')
        series_all_days = None
        if os.path.exists(file_path):
            df = pd.read_excel(file_path, index_col=0)
            series_all_days = pd.Series(df.day, index=df.index, name='day')
        else:
            days = get_all_trade_days()
            series_all_days = pd.Series([x.strftime('%Y-%m-%d') for x in days], index=pd.to_datetime(days), name='day')
            series_all_days.to_excel(file_path)
        # 判断是否跨年，如果跨年，调用 get_all_trade_days 补充新一年的交易日数据
        # 如果没跨年，那读取缓存即可（每年不到年底时，trade_days 就会给到 12月31日的）
        year = str(datetime.now().year)
        try:
            series_all_days[year]
        except:
            # 如果崩溃，说明跨年，应该更新
            days = get_all_trade_days()
            series_all_days = pd.Series([x.strftime('%Y-%m-%d') for x in days], index=pd.to_datetime(days), name='day')
            series_all_days.to_excel(file_path)
            print('交易日数据已跨年，需要更新')
        return series_all_days

    def get_nav_info(self, cache = True):
        """
        获取历史上所有基金的交易净值（买入、卖出、分红等）
        TODO 转托管这里好像，不能裸替了事
        """
        print(get_query_count())
        # 说一下逻辑：
        # 1. 从全家所有成交记录中，取出每一只基金的全部记录，并按交易日期去重（买入和卖出，分红因为除权日问题，需要从分红表中取得）
        # 2. 因为转托管不是真实价格，所以忽略，分红需要除权日数据，也暂时忽略
        # 3. 创建月末标记列，用于将来做月底净值统计之用。

        df_results = None
        file_path = path.join(self.folder, '基金净值数据.xlsx')
        if cache and os.path.exists(file_path):
            dtypes = {
                'code': str
            }
            df = pd.read_excel(file_path, dtype = dtypes, index_col=0)
            return df
        # 所有成交记录（买卖、分红、转托管）
        df = self.rc_loader.get_all_users_combine_records()
        # 无法统计股票
        df = df[~(df['category3'] == '股票')]
        df_code_names = df[['code', 'name']]
        # df_code_names = df_code_names[df_code_names['code'] == '502010']
        df_code_names.drop_duplicates(subset=['code'], inplace=True)
        # 分红表（带缓存）
        df_divid = self.get_divid_info(cache = True)
        code_index = 0
        for item in df_code_names.itertuples():
            code = item.code
            name = item.name
            # 忽略分红和转托管
            df_fund_trade = df[(df.code == code) & ~(df.deal_type == '分红') & ~(df.deal_type == '托管转出') & ~(df.deal_type == '托管转入')]
            df_fund_trade = df_fund_trade[['date', 'deal_type']]
            # 去掉重复交易日期
            df_fund_trade = df_fund_trade.drop_duplicates(subset=['date'])
            fund_trade_days = df_fund_trade.date.unique().tolist()
            # 开始日期
            start_date = fund_trade_days[0]
            start_year = int(start_date[0:4])
            start_month = int(start_date[5:7])
            # start_day = int(start_date[8:10])
            # 结束日期
            end_date = datetime.now().strftime('%Y-%m-%d')
            end_year = int(end_date[0:4])
            end_month = int(end_date[5:7])
            # 分红确认日
            # 注：如果这里崩溃，大概率是 df_divid 分红表获取失败导致，目前的修复方法是，分红表获取方法 cache = True
            df_fund_divid = df_divid[(df_divid['基金代码'] == code) & (df_divid['权益登记日'] >= start_date)]
            divid_days = [x[0:10] for x in df_fund_divid['权益登记日'].tolist()]
            df_fund_divid = df_fund_divid.rename(columns={'权益登记日': 'date', '事项名称':'deal_type'})
            df_fund_divid['date'] = df_fund_divid['date'].apply(lambda x: str(x)[0:10])
            df_fund_divid['deal_type'] = df_fund_divid['deal_type'].apply(lambda x: '分红')
            df_fund_divid = df_fund_divid[['date','deal_type']]
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
            for day in date_range:
                end_date = datetime.strptime(day, '%Y-%m-%d') - timedelta(days=1)
                # 以月末为 end_date 向前去一天交易日数据（可以是月末这一天，也可以是之前的非休息日）
                jq_eof_month_trade_days.append(get_trade_days(end_date=end_date, count=1).tolist()[0].strftime('%Y-%m-%d'))
            # 生成月末日期 DataFrame
            df_fund_eof_month = pd.DataFrame({'date': jq_eof_month_trade_days, 'month_end': ['月末' for x in range(len(jq_eof_month_trade_days))]})
            # 合并交易、分红、月底
            df_all_needed_days = pd.concat([df_fund_trade, df_fund_divid], ignore_index=True)
            df_all_needed_days.sort_values(['date'], inplace=True)
            df_all_needed_days = pd.merge(df_all_needed_days, df_fund_eof_month, on='date', how='outer')
            df_all_needed_days.sort_values(['date'], inplace=True)
            df_all_needed_days.fillna(value={'month_end':'','deal_type':''}, inplace=True)
            df_all_needed_days.reset_index(drop=True, inplace=True)
            print('获取 {0} 净值。进度 {1} / {2}，共计 {3} 天'.format(code + ' ' + name, code_index + 1, len(self.unique_fund_codes), len(df_all_needed_days.date.tolist())))
            code_index += 1
            # if code_index == 5:
            #     break
            # 取净值
            # 查询净值（交易日（买入、卖出、分红），月末净值（统计用））
            nav_query = query(finance.FUND_NET_VALUE).filter(
                finance.FUND_NET_VALUE.code == code,
                finance.FUND_NET_VALUE.day.in_(df_all_needed_days.date.tolist())
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
            # 默认 datetime.date 类型。不转换，就无法对比
            df_jq_fund_nav['交易日'] = df_jq_fund_nav['交易日'].apply(lambda x: x.strftime('%Y-%m-%d'))
            df_jq_fund_nav = pd.merge(df_jq_fund_nav, df_all_needed_days, left_on='交易日', right_on='date', how='outer')
            df_jq_fund_nav = df_jq_fund_nav.rename(columns={'基金代码': 'code', '单位净值':'nav_unit', '累计净值':'nav_acc'})
            df_jq_fund_nav['name'] = name
            # 只要需要的列
            df_jq_fund_nav = df_jq_fund_nav[['code', 'name', 'date', 'nav_unit', 'nav_acc', 'deal_type', 'month_end']]
            df_results = pd.concat([df_results, df_jq_fund_nav], ignore_index=True)
        df_results.sort_values(['code','date'], inplace=True)
        df_results.reset_index(drop=True, inplace=True)
        # 最后多了一些没有净值和 deal_type 的数据，暂时不知道为啥
        df_results.dropna(subset=['nav_unit'], inplace=True)
        # 内部函数，502010 证券公司，这样的数据，库里没有累计净值
        def _fill_acc(x):
            if pd.isna(x.nav_acc):
                return x.nav_unit
            else:
                return x.nav_acc
            pass
        df_results['nav_acc'] = df_results.apply(_fill_acc, axis=1)
        df_results.to_excel(file_path)
        pass

    def get_divid_info(self, cache = True):
        """
        获取历史上所有交易过的基金的分红、拆分数据
        """
        divid_file_path = path.join(self.folder, '分红拆分记录.xlsx')
        if cache and os.path.exists(divid_file_path):
            dtypes = {
                '公布日期': str,
                '基金代码': str,
                '权益登记日': str,
                '除息日': str
            }
            df = pd.read_excel(divid_file_path, dtype = dtypes, index_col=0)
            return df
        print(get_query_count())
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
        # 注：聚宽数据库不全，例如 2020年9月22日 时，502010 在 2020年7月7日的拆分信息就没有在库中，只能自补
        # 但是，重复信息也不影响使用
        addition_divid_file_path = path.join(self.folder, '自补分红拆分记录.xlsx')
        if os.path.exists(addition_divid_file_path):
            dtypes = {
                '公布日期': str,
                '基金代码': str,
                '权益登记日': str,
                '除息日': str
            }
            df_addition_dividend = pd.read_excel(addition_divid_file_path, dtype = dtypes, index_col=0)
            df_jq_dividend_split = df_jq_dividend_split.append(df_addition_dividend, ignore_index=True)
            df_jq_dividend_split.sort_values(['基金代码','公布日期'], inplace=True)
        df_jq_dividend_split.to_excel(path.join(self.folder, '分红拆分记录.xlsx'))
        

        return df_jq_dividend_split

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

    def get_fund_operate_type(self):
        """
        获取基金的交易方式（开放式、LOF、ETF、QDII 等。FOF 是持有基金的基金，感觉用处不大，应该不会买）
        """
        # 1. 获取支持查询的基金代码表
        df = self.cm.df_category
        df_sub = df[~(df['市场'] == '不适用')]
        df_sub = df[~(df['三级分类'] == '股票')]
        code_list = df_sub.基金代码.unique().tolist()
        # 2. 查询
        df_jq = finance.run_query(
            query(
                finance.FUND_MAIN_INFO
            ).filter(
                finance.FUND_MAIN_INFO.main_code.in_(code_list)
            )
        )
        # 3. 合并且调整格式
        df_results = pd.merge(df, df_jq, left_on='基金代码', right_on='main_code', how='left')
        cols = df.columns.tolist()
        cols.append('operate_mode')
        df_results = df_results[cols]
        df_results.rename(columns={'operate_mode':'基金类型'}, inplace=True)
        df_results.fillna(value={'基金类型':'不适用'}, inplace=True)
        df_results.replace({'开放式基金':'场外'}, inplace=True)
        df_results = df_results[['基金名称', '基金简称', '基金代码', '基金类型', '市场', '一级分类', '二级分类', '三级分类', '分类ID']]
        self.cm.df_category = df_results.copy()
        # 4. 保存
        self.cm.save_category_file()

        return df_results

    def update_nav_of_all_records(self):
        """
        1. 更新所有需要调整的 APP 的净值和日期数据（天天、且慢、蛋卷，其他不用）
        2. 更新整体表
        """
        file_paths = []
        output_path = path.join(project_folder, 'output')
        for root, dirs, files in os.walk(output_path):
            for f in files:
                # 最终结果产出文件（忽略中间过程文件）
                if f.startswith(u'03'):
                    should_select = False
                    for app in ['天天', '且慢', '蛋卷']:
                        should_select = should_select or app in f
                    if should_select:
                        file_paths.append(path.join(root, f))
        # 聚宽净值
        df_nav = self.get_nav_info(cache = True)
        df_nav.date = df_nav.date.astype(np.datetime64)
        # 内部函数
        def calc_date(x):
            op = x['deal_type']
            if op == '分红':
                # 日期游标（除权日，可能在给定日期前后的一个范围内）
                # 非货币基金的分红，通常稍有一个月触发 2 次及以上的
                date_cursor = x['date']
                # 上边界，15 天以前
                before = pd.DatetimeIndex(end=date_cursor, freq='D', periods=15)[0]
                # 下边界 15 天以后
                after = pd.DatetimeIndex(start=date_cursor, freq='D', periods=15)[-1]
                
                df_temp = df_nav[(df_nav['code'] == x['code']) & (df_nav['date'] >= before) & (df_nav['date'] <= after) & (df_nav['deal_type'] == '分红')]
                # print(date_cursor, before, after, df_temp)
                if len(df_temp) > 0:
                    return str(df_temp.date.values[0])[0:10]
                else:
                    return '无法补充'
                pass
            else:
                return x['date']
            pass

        def calc_nav_unit(x):
            df_temp = df_nav[(df_nav['code'] == x['code']) & (df_nav['date'] == x['date'])]
            # print(df_temp)
            if len(df_temp) > 0:
                return df_temp.nav_unit.values[0]
            else:
                # return '无法补充'
                return x['nav_unit']

        def calc_nav_acc(x):
            df_temp = df_nav[(df_nav['code'] == x['code']) & (df_nav['date'] == x['date'])]
            # print(df_temp)
            if len(df_temp) > 0:
                return df_temp.nav_acc.values[0]
            else:
                # return '无法补充'
                return x['nav_acc']
        # 需要调整的文件
        for file_path in file_paths:
            print('处理：{0} ...'.format(file_path))
            df_r = pd.read_excel(file_path, index_col=0, dtype={'code':str})
            df_r['date'] = df_r.apply(calc_date, axis=1)
            df_r['nav_unit'] = df_r.apply(calc_nav_unit, axis=1)
            df_r['nav_acc'] = df_r.apply(calc_nav_acc, axis=1)
            df_r.to_excel(file_path)

if __name__ == "__main__":
    jq = jqadapter()
    # jq.get_trade_day_info()
    # jq.get_divid_info(cache = False)
    # jq.get_nav_info(cache = False)
    # jq.get_fund_operate_type()
    jq.update_nav_of_all_records()