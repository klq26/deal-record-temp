import os
import sys
import pandas as pd
import numpy as np

from recordloader import recordloader

pd.set_option('display.max_columns', 30)
pd.set_option('display.max_rows', 1100)

class dealhistory:

    def __init__(self):
        self.folder = os.path.abspath(os.path.dirname(__file__))
        self.rc_loader = recordloader()
        pass

    def calc(self, user=None, code=''):
        """
        根据基金代码，进行历史交易计算，得出分红、总盈亏、总费用等数据。
        如果不写 user，则将自己和父母的一起统计
        """
        df_records = None
        if user == None:
            df_records = self.rc_loader.get_all_users_combine_records()
        elif user not in ['klq', 'parents'] or code == '':
            return
        elif user == 'klq':
            self.rc_loader.set_user_id('klq')
            df_records = self.rc_loader.get_records()
        elif user == 'parents':
            self.rc_loader.set_user_id('parents')
            df_records = self.rc_loader.get_records()
        # 开始计算 #

        # 不要股票
        df_records = df_records[~(df_records.category3 == '股票')]
        # 取出特定基金的记录
        df_records = df_records[df_records.code == code]
        # 每一个步骤（买入、卖出、分红等）
        steps = []
        history_gain = 0
        holding_nav = 0
        holding_volume = 0
        total_fee = 0
        money_divid = 0     # 现金分红
        volume_divid = 0    # 红利再投资
        status = ''         # 初始化？清仓？买入卖出？
        initialized = False
        for x in df_records.itertuples(index=False):
        #     print(x)
        #     print(f'☆ status: {x.deal_type}, operate_nav: {x.nav_unit}, volume: {x.volume}, total_fee:{x.fee}, date: {x.date}\n')
            op = x.deal_type
            nav = round(x.nav_unit, 3)
            vol = round(x.volume, 2)
            # 初始化建仓
            if not initialized:
                holding_volume = x.volume
                if op == '买入':
                    holding_nav = x.nav_unit
                    status = '初始'
                    initialized = True
                elif op == '分红': # 目前仅兼容华宝油气
                    if holding_volume > 0:
                        holding_nav = x.nav_unit
                    else:
                        holding_nav = 0.0
                        if x.volume > 0:
                            # 红利再投资，如果是红利再投资，上面的 holding_volume = x.volume 已经去到份额了
                            history_gain += round(x.nav_unit * x.volume, 2)
                            volume_divid += round(x.volume, 2)
                            pass
                        elif x.occur_money > 0:
                            # 现金分红
                            history_gain += round(x.occur_money, 2)
                            money_divid += round(x.occur_money, 2)
                            pass
                        status = '分红'
                total_fee += x.fee
            elif op in ['买入', '托管转入']:
                holding_nav = round((holding_nav * holding_volume + x.nav_unit * x.volume) / (holding_volume + x.volume), 3)
                holding_volume += x.volume
                total_fee += x.fee
                status = op
                pass
            elif op == '分红':
                if x.volume > 0:
                    # print('红利再投资')
                    history_gain += round(x.nav_unit * x.volume, 2)
                    volume_divid += round(x.volume, 2)
                    pass
                elif x.occur_money > 0:
                    # print('现金分红')
                    history_gain += round(x.occur_money, 2)
                    money_divid += round(x.occur_money, 2)
                    pass
                status = '分红'
                pass
            elif op in ['卖出', '托管转出']:
                total_fee += x.fee
                if holding_volume <= np.abs(x.volume):
                    history_gain += round((x.nav_unit - holding_nav) * np.abs(x.volume), 3)
                    holding_nav = 0
                    holding_volume = 0
                    status = '清仓'
                    initialized = False
                else:
                    history_gain += round((x.nav_unit - holding_nav) * np.abs(x.volume), 3)
                    holding_nav = round((holding_nav * holding_volume - x.nav_unit * np.abs(x.volume)) / (holding_volume - np.abs(x.volume)), 3)
                    holding_volume -= np.abs(x.volume)
                    status = op
                pass
            # 格式化
            holding_nav = round(holding_nav, 3)
            holding_volume = round(holding_volume, 2)
            total_fee = round(total_fee, 2)
            history_gain = round(history_gain, 2)
        #     print(f'★ status: {status}, holding_nav: {holding_nav}, holding_volume: {holding_volume}, total_fee:{total_fee}, history_gain: {history_gain}\n')
            steps.append({
                'date':x.date, 
                'code':x.code, 
                'name':x.name, 
                'status':status, 
                'holding_nav':holding_nav, 
                'holding_volume': holding_volume, 
                'total_fee': total_fee, 
                'money_divid':money_divid, 
                'volume_divid':volume_divid, 
                'history_gain':history_gain
            })
        pd_results = pd.DataFrame(steps, columns=['date', 'code', 'name', 'status', 'holding_nav', 'holding_volume','total_fee','money_divid','volume_divid','history_gain'])
        return pd_results

if __name__ == "__main__":
    dh = dealhistory()
    df = dh.calc(user='klq',code = '100032')
    print(df)
