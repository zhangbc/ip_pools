# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
多进程抓取IP归属地并入库MYSQL
author:         zhangbc
last modify:    2017-04-21
"""


import sys
import random
import requests
import re
import time
import multiprocessing
import cymysql
import settings
from utils.ip_processor import IpProcessor


def save_to_mysql(sql):
    """
    将采集到的Ip信息入库操作
    :param sql: SQL语句
    :return:
    """

    try:
        conn = cymysql.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER,
                               passwd=settings.MYSQL_PASSWD, db=settings.MYSQL_DBNAME,
                               port=settings.MYSQL_PORT, charset=settings.MYSQL_CHARSET)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
    except cymysql.Error as ex:
        print 'MySQL connecting error,reason is:'+str(ex[1])
        sys.exit()


def get_ip_info(ip):
    """
    从百度获取IP的地址信息
    :param ip: IP地址
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

    contents = rs.get(url=url, headers=headers).content.replace('IP地址:&nbsp;', '')

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
        save_to_mysql(sql=insert_sql)

    time.sleep(random.uniform(0, 1))


def work(ia, ib):
    """
    实现函数, work
    :return:
    """

    pool = multiprocessing.Pool(processes=settings.PROCESSES)
    while True:
        ips = IpProcessor().get_deletion_ips(ia, ib)
        if not len(ips):
            print u'-------{0}.{1}.x.x段没有待抓取--------'.format(ia, ib)
            break

        print u'-------{0}.{1}.x.x段共有{2}个IP待抓取--------'.format(ia, ib, len(ips))
        for ip in ips:
            try:
                pool.apply_async(get_ip_info, (ip,))
            except multiprocessing.ProcessError:
                pass

        pool.close()
        pool.join()


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:' + start_time
    work(1, 20)
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'End:%s' % end_time
