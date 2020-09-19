import os
import sys
from os import path
import json
import pandas as pd
import numpy as np
import grequests

# 加根目录到 sys.path
project_folder = os.path.abspath(os.path.join(path.dirname(__file__), os.pardir))
if project_folder not in sys.path:
    sys.path.append(project_folder)

from category.categories import categories

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class fundinfo:

    def __init__(self):
        self.folder = os.path.abspath(os.path.dirname(__file__))
        # 分类信息
        self.cm = categories()
        df_category = self.cm.df_category
        self.headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36', 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'accept-encoding': 'gzip, deflate, br'}
        # 不要冻结资金、现金类的
        df_category = df_category[(~df_category['一级分类'].isin(['冻结资金','现金']))]
        # 也不要股票的
        df_category = df_category[(~df_category['二级分类'].isin(['股票']))]
        self.code_list = df_category.基金代码.unique().tolist()
        pass

    def get_fund_operate_info(self, cache = True):
        """
        获取资产配置分类表中，所有支持基金（去除货币基金和虚拟代码，去除股票）的销售运营数据
        """
        file_path = path.join(self.folder, '基金运营数据.xlsx')
        if cache and os.path.exists(file_path):
            return pd.read_excel(file_path, index_col=0, dtype={'基金代码':str})

        # 指数型、美国 QDII 型、债券型（注意，香港 QDII 买入确认日也是 1 天，一切以运营数据为准）
        # self.code_list = ['000968','162411','003376']

        url_holder1 = u'https://danjuanapp.com/djapi/fund/{0}'
        url_holder2 = u'https://danjuanapp.com/djapi/fund/detail/{0}'

        urls1 = [url_holder1.format(x) for x in self.code_list]
        urls2 = [url_holder2.format(x) for x in self.code_list]

        response_list1 = grequests.map([grequests.get(x, headers = self.headers, callback = self.grequests_get_callback) for x in urls1], size=2, exception_handler = self.grequests_exception_handler)
        response_list2 = grequests.map([grequests.get(x, headers = self.headers, callback = self.grequests_get_callback) for x in urls2], size=2, exception_handler = self.grequests_exception_handler)

        infos1 = []
        infos2 = []
        # 基础信息
        for resp in response_list1:
            if resp and resp.status_code == 200:
                data = json.loads(resp.text)
                if data['result_code'] != 0:
                    # 仅场内销售的基金，如 510500，无法查询（也不需要）
                    continue
                jsonData = data['data']
                d = dict()
                d['基金代码'] = jsonData['fd_code']
                d['基金名称'] = jsonData['fd_name']
                d['基金类型'] = jsonData['type_desc']
                d['基金份额'] = jsonData['totshare']
                infos1.append(d)
        df1 = pd.DataFrame(infos1)
        df1 = df1[['基金代码','基金名称','基金类型','基金份额']]
        # 详细信息
        for resp in response_list2:
            if resp and resp.status_code == 200:
                data = json.loads(resp.text)
                if data['result_code'] != 0:
                    # 仅场内销售的基金，如 510500，无法查询（也不需要）
                    continue
                jsonData = data['data']
                d = dict()
                # 费用
                d['申购费率'] = jsonData['fund_rates']['declare_rate'] + '%'
                d['申购折扣'] = jsonData['fund_rates']['declare_discount']
                infos = ['{0}：{1}%'.format(item['name'],item['value']) for item in jsonData['fund_rates']['other_rate_table']]
                d['持有费率'] = ' / '.join(infos)
                infos = ['{0}：{1}%'.format(item['name'],item['value']) for item in jsonData['fund_rates']['withdraw_rate_table']]
                d['卖出费率'] = ' / '.join(infos)        
                # 时长
                jsonConfirm = jsonData['fund_date_conf']
                d['基金代码'] = jsonConfirm['fd_code']
                d['买入确认日'] = jsonConfirm['buy_confirm_date']
                d['买入查看日'] = jsonConfirm['buy_query_date']
                d['买入总天数'] = jsonConfirm['all_buy_days']
                d['卖出确认日'] = jsonConfirm['sale_confirm_date']
                d['卖出到账日'] = jsonConfirm['sale_query_date']
                d['卖出总天数'] = jsonConfirm['all_sale_days']  
                # 仓位
                jsonPosition = jsonData['fund_position']
                d['股票比例'] = str(jsonPosition.get('stock_percent', 0)) + '%'
                d['债券比例'] = str(jsonPosition.get('bond_percent', 0)) + '%'
                d['现金比例'] = str(jsonPosition.get('cash_percent', 0)) + '%'
                d['其他比例'] = str(jsonPosition.get('other_percent', 0)) + '%'
                d['股票清单'] = jsonPosition.get('stock_list',[])
                d['债券清单'] = jsonPosition.get('bond_list', [])
                infos2.append(d)
        df2 = pd.DataFrame(infos2)
        df2 = df2[['基金代码', '股票比例', '债券比例', '现金比例', '其他比例', '买入确认日', '买入查看日', '买入总天数', '卖出确认日', '卖出到账日', '卖出总天数','申购费率', '申购折扣', '卖出费率', '持有费率', '股票清单', '债券清单']]
        # 整合信息
        df = pd.merge(df1, df2, on='基金代码',how='outer')
        df.to_excel(file_path)
        return df

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
    fi = fundinfo()
    fi.get_fund_operate_info()
