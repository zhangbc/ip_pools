# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
抓取IP归属地并入库MYSQL, 在类中应用多进程
author:         zhangbc
last modify:    2017-04-26
"""


import sys
import random
import requests
import re
import time
import multiprocessing
import settings
from utils.ip_processor import IpProcessor

reload(sys)
sys.setdefaultencoding('utf8')


class CrawlIPs(object):
    """
    采集IP信息入库
    """

    def __init__(self, func):
        self.func = func

    @staticmethod
    def get_ip_info(ip):
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

            IpProcessor().exec_no_query(sql=insert_sql)
        time.sleep(random.uniform(0, 1))

    def work(self):
        """
        采集数据,work
        """

        ia = 1
        ib = 20

        while True:
            ips = IpProcessor().get_deletion_ips(ia, ib)
            if not len(ips):
                print u'-------{0}.{1}.x.x段没有待抓取--------'.format(ia, ib)
                break

            pool = multiprocessing.Pool(processes=settings.PROCESSES)
            print u'-------{0}.{1}.x.x段共有{2}个IP待抓取--------'.format(ia, ib, len(ips))
            print 'Begin:' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            for ip in ips:
                try:
                    pool.apply_async(self.func, (self, ip,))
                except multiprocessing.ProcessError:
                    pass

            pool.close()
            pool.join()


def work_wrap(instance, ip):
    """
    用一个可被实例化的普通函数包装类方法，
    将实例对象作为参数传递给函数即可。
    :param instance:
    :param ip:
    :return:
    """

    return instance.get_ip_info(ip)


def main():
    """
    实现函数
    :return:
    """

    crawl_ips = CrawlIPs(work_wrap)
    crawl_ips.work()

if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}'.format(start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'End:{0}'.format(end_time)
