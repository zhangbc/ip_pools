# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
抓取IP归属地并入库MYSQL
author:         zhangbc
last modify:    2017-04-26
"""


import sys
import random
import requests
import re
import time
import settings
from utils.ip_processor import IpProcessor

reload(sys)
sys.setdefaultencoding('utf8')


class CrawlIPs(IpProcessor):
    """
    采集IP信息入库
    """

    def __init__(self):
        IpProcessor.__init__(self)

    def get_ip_info(self, ip):
        """
        从百度获取IP的地址信息
        :param ip:
        :return:
        """

        rex1 = r'<span class="c-gap-right">([\S]+)</span>([\S]+) ([\S]+)'
        rex2 = r'<span class="c-gap-right">([\S]+)</span>([\S]+)'
        rex3 = r'\d+\.\d+\.\d+\.\d+'
        url = 'https://www.baidu.com/baidu?tn=monline_3_dg&ie=utf-8&wd=%s' % ip

        rs = requests.session()
        headers = {
            "Host": "www.baidu.com",
            "User-Agent": random.choice(settings.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        contents = rs.get(url=url, headers=headers).text.replace('IP地址:&nbsp;', '')

        data = re.findall(rex1, contents)

        # 应该匹配第一个结果结果值. bug修改 by 2017-05-14
        if not data or data[0][0] != ip:
            data = re.findall(rex2, contents)

        if data and re.findall(rex3, data[0][0]):
            ip_dict = dict()
            ip_dict["ip"] = data[0][0]
            ip_dict["addr"] = data[0][1]
            try:
                ip_dict["carrier"] = data[0][2]
            except IndexError:
                ip_dict["carrier"] = ""

            insert_sql = 'insert into ip_info(ip, addr, carrieroperator) VALUES ' \
                         '(\'%(ip)s\', \'%(addr)s\', \'%(carrier)s\')' % ip_dict

            self.exec_no_query(sql=insert_sql)
        time.sleep(random.uniform(0, 1))

    def work(self):
        """
        采集数据,work
        """

        ia = 1
        ib = 0
        while True:
            ips = self.get_deletion_ips(ia, ib)
            if not len(ips):
                print u'-------{0}.{1}.x.x段没有待抓取--------'.format(ia, ib)
                break

            print u'-------{0}.{1}.x.x段共有{2}个IP待抓取--------'.format(ia, ib, len(ips))
            print 'Begin:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            for ip in ips:
                self.get_ip_info(ip)


def main():
    """
    实现函数
    :return:
    """

    crawl_ips = CrawlIPs()
    print u'--------请输入要运行的程序编号--------\n' \
          u'1：IP信息采集；\n2：查询；\n3：检查爬漏IP\n' \
          u'---------------------------------------\n'
    number = sys.argv[1]
    print u'您正在运行的程序编号为：{0}'.format(number)

    if number == "1":
        crawl_ips.work()
        print 'Done!'
    elif number == "2":
        rows = crawl_ips.get_ip_by_condition('*', 'and ip like \'1.8.%\';')
        print len(rows)
    elif number == "3":
        crawl_ips.get_deletion_ips(1, 3)
    else:
        print u'输入有误，请检查！'


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}'.format(start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'End:{0}'.format(end_time)
