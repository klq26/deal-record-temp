# -*- coding: utf-8 -*-

import os
from os import path
import sys
import json

from abc import ABC, abstractmethod

import pandas as pd
import numpy as np

from record import record, record_keys

# 加根目录到 sys.path
sys.path.append(path.abspath(path.dirname(path.dirname(__file__))))

from category.categories import categories
from login.webcookies import webcookies

class absspider(ABC):
    """
    所有网站爬虫的抽象父类
    """

    ##################
    # 公共属性
    ##################
    # 基金分销商名称，如：天天基金、蛋卷基金等
    app_name = '未设置'
    # 用户名
    user_name = '康力泉'
    # 日期类型（天天基金是确认日，且慢是申请日（但是要判断时间，如15:01:00 要错后一天），蛋卷基金是正确的申请日）
    date_type = ''
    # 当前文件夹
    folder = path.abspath(path.dirname(__file__))
    # 分类信息
    cm = categories()
    # 请求头
    wm = webcookies()
    # 输入文件名
    input_file_name = ''
    # 每一列预期的数据类型
    suppose_dtypes = {'id':np.int64, 'date':str, 'time':str, 'code':str, 'name':str, 'deal_type':str, 'nav_unit':np.float64, 'nav_acc':np.float64,'volume':np.float64, 'deal_money':np.float64, 'fee':np.float64, 'occur_money':np.float64, 'account':str, 'unique_id':str, 'category1':str, 'category2':str, 'category3':str, 'category_id':np.int64,'note':str}

    df_trade_list = pd.DataFrame()
    df_raw_record_list = pd.DataFrame()
    df_results = pd.DataFrame()

    @property
    def output_folder(self):
        """
        输出文件夹
        """
        output_path = path.join(path.abspath(path.dirname(self.folder)), 'output', self.app_name)
        if not path.exists(output_path):
            os.makedirs(output_path)
        return output_path
    
    @property
    def input_file(self):
        """
        输入文件
        """
        input_path = path.join(path.abspath(path.dirname(self.folder)), 'input', self.app_name)
        if not path.exists(input_path):
            os.makedirs(input_path)
        if len(self.input_file_name) == 0:
            return ''
        else:
            return path.join(input_path, self.input_file_name)

    ##################
    # 实例方法
    ##################

    def get(self):
        """
        完整流程
        """
        # 1. 成交列表
        print('{0} {1} 正在获取..'.format(self.app_name, self.user_name))
        print('获取交易列表')
        self.get_trade_list()
        if len(self.df_trade_list) > 0:
            self.save_trade_list()
        # 2. 成交记录原始数据
        print('获取原始交易记录')
        self.get_raw_record_list()
        if len(self.df_raw_record_list) > 0:
            self.save_raw_record_list()
        # 3. 清洗后的成交记录
        print('整理可用交易记录')
        self.get_records()
        if len(self.df_results) > 0:
            if self.date_type != '':
                # 天天 - 提前一天
                # 且慢 - 看时间，如果大于 15 点，后搓一天
                # 蛋卷 - 无需处理
                self.adjust_dates()
            self.save_record_list()

    def save_trade_list(self):
        """
        保存交易列表
        """
        self.df_trade_list.to_excel(path.join(self.output_folder, '01_{0}_trade_list.xlsx'.format(self.user_name)), sheet_name='{0}_交易列表'.format(self.user_name))

    def save_raw_record_list(self):
        """
        保存未清洗的成交记录数据
        """
        self.df_raw_record_list.to_excel(path.join(self.output_folder, '02_{0}_raw_record_list.xlsx'.format(self.user_name)), sheet_name='{0}_交易记录'.format(self.user_name))

    def save_record_list(self):
        """
        保存交易记录
        """
        self.df_results.to_excel(path.join(self.output_folder, '03_{0}_{1}.xlsx'.format(self.user_name, self.app_name)), sheet_name='{0}_交易记录'.format(self.user_name))

    ##################
    # 抽象方法
    ##################
    @abstractmethod
    def get_trade_list(self):
        """
        Step 1. 获取交易列表记录原始数据（每条记录应有唯一 id，一条交易列表记录应对与 0 ~ n 条基金成交记录）
        """
        pass
    
    @abstractmethod
    def get_raw_record_list(self):
        """
        Step 2. 获取未经清洗的基金成交记录（每条记录应有唯一 id）
        """
        pass

    @abstractmethod
    def get_records(self):
        """
        Step 3. 获取基金成交记录（每条记录应有唯一 id）
        """
        pass
    
    @abstractmethod
    def adjust_dates(self):
        """
        *Step 4. 部分场外基金，需要对操作日期进行调整
        """
        pass