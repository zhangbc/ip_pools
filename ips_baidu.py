# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
多进程抓取IP归属地并入库MYSQL
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
        time.sleep(1)

    @staticmethod
    def get_ips(ia, ib, ic, ie):
        """
        获取批量IP地址
        :param ia: IP地址的A段, 若为0，则0~255
        :param ib: IP地址的B段, 若为0，则0~255
        :param ic: IP地址的C段, 若为0，则0~255
        :param ie: IP地址的D段, 若为0，则0~255
        :return: 返回IP值数组
        """

        radix = xrange(0, 256)
        if ia > 0:
            ia = xrange(ia, ia+1)
        else:
            ia = [i for i in radix if i > 0]

        if ib > 0:
            ib = xrange(ib, ib+1)
        else:
            ib = [0]

        if ic > 0:
            ic = xrange(ic, ic+1)
        else:
            ic = radix

        if ie > 0:
            ie = xrange(ie, ie+1)
        else:
            ie = radix

        ips = [str(x)+"."+str(y)+"."+str(m)+"."+str(n)
               for x in ia for y in ib for m in ic for n in ie]
        return ips

    def get_deletion_ips(self, ia, ib):
        """
        获取缺失的IP(爬漏检查)
        :param ia: IP的A段
        :param ib: IP的B段
        :return:
        """

        ips = self.get_ips(ia, ib, 0, 0)
        rows = self.get_ip_by_condition('*', 'and ip like \'{ia}.{ib}.%\';'.format(ia=ia, ib=ib))
        ips_data = [row[1] for row in rows]
        deletion_ips = list(set(ips).difference(set(ips_data)))
        return deletion_ips

    def crawl_ip_info(self):
        """
        采集数据
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
    实现主体函数，多进程
    :return:
    """

    crawl_ips = CrawlIPs()
    print u'--------请输入要运行的程序编号--------\n' \
          u'1：多进程采集；\n2：查询；\n3：检查爬漏IP\n' \
          u'---------------------------------------\n'
    number = sys.argv[1]
    print u'您正在运行的程序编号为：{0}'.format(number)

    if number == "1":
        crawl_ips.crawl_ip_info()
        print 'Done!'
        # pool = multiprocessing.Pool(processes=settings.PROCESSES)
        # ips = crawl_ips.get_deletion_ips(1, 1)
        # print ips
        # for ip in ips:
        #     try:
        #         pool.apply_async(crawl_ips.get_ip_info, (ip,))
        #     except Exception as ex:
        #         print ex
        #
        # pool.close()
        # pool.join()
    elif number == "2":
        rows = crawl_ips.get_ip_by_condition('and ip like \'1.8.%\' limit 1;')
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
