# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
将ip_info中归中属地和运营商信息
提取并写入对应的表
author:         zhangbc
create_time:    2017-05-19
"""

import sys
import time
from utils.ip_processor import IpProcessor


reload(sys)
sys.setdefaultencoding('utf8')


class UpdateCityCarrier(IpProcessor):
    """
    将ip_info中归中属地和运营商信息提取并写入对应的表
    """

    def __init__(self):
        IpProcessor.__init__(self)

    def update_city(self):
        """
        更新ip_city信息
        :return:
        """
        columns = 'distinct addr'
        condition = 'and ip like \'1.10.%\''
        data = self.get_ip_by_condition(columns=columns, condition=condition)

        if len(data):
            for index, dt in enumerate(data):
                print dt[0]

            print u''.join(['(\'{0}\'),'.format(dt[0]) for dt in data])[:-1]

        # insert_sql = 'insert into ip_city(city) VALUES {0}'.format(data)

    def update_carrier(self):
        """

        :return:
        """
        pass

    def get_count(self, flag):
        """
        按照IP的AB段分类统计或者统计总数
        :param flag: flag=0表示统计总数，flag=1表示分类统计
        :return:
        """

        if flag == 0:
            result = self.get_total_ips()
            print result
        elif flag == 1:
            result = self.get_count_by_group()
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

    updates = UpdateCityCarrier()
    updates.update_city()


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}'.format(start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}\nEnd:{1}'.format(start_time, end_time)
