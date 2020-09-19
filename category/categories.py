# -*- coding: utf-8 -*-
import os
import sys
import json

import pandas as pd
import numpy as np

class categories:
    """
    自建交易品种分类列表，包含从投资以来，所有交易过的品种，支持返回对应代码的分类信息。
    """

    def __init__(self):
        self.folder = os.path.abspath(os.path.dirname(__file__))
        # 读取最新分类表
        self.df_category = self.refresh_category_file()
        # 读取最新三级分类扩展表（扩展到 xueqiu symbol 这样的内容）
        self.df_category3_ext = self.refresh_category3_extension_file()
        pass

    def save_category_file(self):
        """
        把内存中修改过的文件，存入磁盘
        """
        self.xlsx_path = os.path.join(self.folder, u'资产配置分类表.xlsx')
        self.df_category.to_excel(self.xlsx_path, sheet_name='资产分类')
        pass

    def refresh_category_file(self):
        """
        加载三级分类表
        """
        self.xlsx_path = os.path.join(self.folder, u'资产配置分类表.xlsx')
        self.df_category = pd.read_excel(self.xlsx_path, dtype={'基金代码': str, '分类ID': np.int64})
        return self.df_category

    def refresh_category3_extension_file(self):
        """
        加载三级分类扩展文件（支持蛋卷估值对应、雪球 + 东方财富指数符号对应）
        """
        self.xlsx_path = os.path.join(self.folder, u'三级分类扩展表.xlsx')
        self.df_category3_ext = pd.read_excel(self.xlsx_path, dtype={'指数代码': str, '顺序': np.int64})
        return self.df_category3_ext

    def get_money_fund_category(self):
        """
        获取货币基金的虚拟类别
        """
        return self.get_category_by_code('999999')

    def get_category_by_code(self, code):
        """
        获取对应基金的类别代码
        """
        result = list(self.df_category[self.df_category['基金代码'] == code].values)
        if len(result) > 0:
            result = result[0]
            return {'category1': result[3], 'category2': result[4], 'category3': result[5], 'categoryId': result[6]}
        else:
            return {'category1': '', 'category2': '', 'category3': '', 'categoryId': ''}

if __name__ == "__main__":
    cm = categories()