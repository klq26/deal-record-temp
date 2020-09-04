# -*- coding: utf-8 -*-
import os
import sys

class webcookies:
    """
    获取各个网站的用户 cookie 帮助类
    """

    def __init__(self):
        self.folder = os.path.abspath(os.path.dirname(__file__))

    def get_danjuan_klq(self):
        return self.get_headers(os.path.join(self.folder, u'danjuan_klq.txt'))

    def get_danjuan_lsy(self):
        return self.get_headers(os.path.join(self.folder, u'danjuan_lsy.txt'))

    def get_danjuan_ksh(self):
        return self.get_headers(os.path.join(self.folder, u'danjuan_ksh.txt'))

    def get_tiantian_klq(self):
        return self.get_headers(os.path.join(self.folder, u'tiantian_klq.txt'))

    def get_tiantian_lsy(self):
        return self.get_headers(os.path.join(self.folder, u'tiantian_lsy.txt'))

    def get_qieman_klq(self):
        return self.get_headers(os.path.join(self.folder, u'qieman_klq.txt'))

    def get_qieman_ksh(self):
        return self.get_headers(os.path.join(self.folder, u'qieman_ksh.txt'))

    def get_headers(self, filepath):
        """
        从文件中读取 request header 原始内容，拼装成 dict 对象共 requests 等模块使用
        """
        headers = {}
        with open(filepath, 'r', encoding=u'utf-8') as f:
            lines = f.readlines()
            cookies = []
            for line in lines:
                if line.lower().startswith('cookie'):
                    cookies.append(line)
            # 随手记用 Charles 抓取的 cookie 是多行的，需要整合
            cookieValue = ''
            if len(cookies) > 1:
                cookieValue = cookies[0][7:len(cookies[0])].replace('\n', '')
                # print(cookieValue)
                for i in range(1, len(cookies)):
                    cookieValue = cookieValue + \
                        '; {0}'.format(
                            cookies[i][7:len(cookies[i])].replace('\n', ''))
            for i in range(1, len(lines)):
                line = lines[i].strip('\n')
                values = []
                if '\t' in line:
                    values = line.split('\t')
                elif ': ' in line:
                    values = line.split(': ')
                else:
                    values = []
                headers[values[0].replace(':', '')] = values[1]
            if cookieValue != '':
                headers['cookie'] = cookieValue
        return headers


if __name__ == '__main__':
    wc = webcookies()
    print(wc.get_danjuan_klq())
    print('\n')
    print(wc.get_danjuan_lsy())
    print('\n')
    print(wc.get_danjuan_ksh())
    print('\n')
    print(wc.get_tiantian_klq())
    print('\n')
    print(wc.get_tiantian_lsy())
    print('\n')
    print(wc.get_qieman_klq())
    print('\n')
    print(wc.get_qieman_ksh())