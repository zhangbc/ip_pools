# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
将ip_info中归中属地和运营商信息提取并写入对应的表
author:         zhangbc
create_time:    2017-05-19
"""

import sys
import time
from db_mysql import MysqlDB


reload(sys)
sys.setdefaultencoding('utf8')


class UpdateCites(object):
    """
    将ip_info中归中属地和运营商信息提取并写入对应的表
    """

    def __init__(self):
        self.mysql = MysqlDB()

    def update_city(self):
        pass

    def update_carrier(self):
        pass

    def get_count(self, flag):
        """
        按照IP的AB段分类统计或者统计总数
        :param flag: flag=0表示统计总数，flag=1表示分类统计
        :return:
        """

        if flag == 0:
            result = self.mysql.get_total_ips()
            print result
        elif flag == 1:
            result = self.mysql.get_count_by_group()
            for index, row in enumerate(result):
                print row[0], row[1]
        else:
            print u'输入参数有误，请检查！'
            sys.exit()


def main():
    """
    函数实现
    :return:
    """

    update_city = UpdateCites()
    update_city.get_count(1)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}'.format(start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}\nEnd:{1}'.format(start_time, end_time)
