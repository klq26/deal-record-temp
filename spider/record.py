# -*- coding: utf-8 -*-


def record_keys():
    """
    DataFrame 重排的类名及顺序
    """
    return ['id', 'date', 'time', 'code', 'name', 'deal_type', 'nav_unit', 'nav_acc', 'volume', 'deal_money', 'fee', 'occur_money', 'account', 'category1', 'category2', 'category3', 'category_id', 'unique_id', 'note']

class record(object):

    def __init__(self, data = None):
        # 序号
        self.id = 1
        # 交易日期
        self.date = '1970-01-01'
        # 交易时间
        self.date = '9:30:00'
        # 代码
        self.code = u'000000'
        # 名称
        self.name = u'默认名称'
        # 交易类型：买入，卖出，分红
        self.deal_type = '买入'
        # 当前净值
        self.nav_unit = 0.0000
        # 累计净值
        self.nav_acc = 0.0000
        # 成交份额
        self.volume = 0.00
        # 实际交易金额
        self.deal_money = 0.00
        # 手续费
        self.fee = 0.00
        # 发生金额 or 过手金额（买入多花，卖出少得。买入时等于 净值 * 份额 + fee。卖出时等于 净值 * 份额 - fee。）
        self.occur_money = 0.00
        # 交易账户
        self.account = '华泰证券'
        # 来源网站中的唯一识别 id
        self.unique_id = '唯一值'
        # 自定义类别，便于筛选汇总
        self.category1 = '类别1'
        self.category2 = '类别2'
        self.category3 = '类别3'
        self.category_id = '类别Id'
        # 详细说明
        self.note = '详细说明'
        
        if data:
            self.__dict__ = data

    def __str__(self):
        """
        输出对象
        """
        return str(self.__dict__)
    
    def __getitem__(self, key):
        return getattr(self, key)
    
    def __setitem__(self, key, value):
        setattr(self, key, value)
    