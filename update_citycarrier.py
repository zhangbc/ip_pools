# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
将ip_info中归中属地和运营商信息
提取并写入对应的表，并写入日志表
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

    def update_city(self, ia=0, ib=0):
        """
        更新ip_city信息
        :param ia: IP的A段地址
        :param ib: IP的B段地址
        :return:
        """

        condition = 'ip like \'{0}.{1}.%\''.format(ia, ib)
        insert_city_sql = (r'''INSERT INTO ip_city(city) '''
                       '''SELECT DISTINCT addr FROM ip_info '''
                       '''WHERE addr NOT IN (SELECT city FROM ip_city) '''
                       '''AND {0};'''.format(condition))
        row_count = self.exec_no_query(insert_city_sql)

        query_city_sql = (r'''SELECT DISTINCT addr FROM ip_info '''
                       '''WHERE {0};'''.format(condition))
        city_count = len(self.exec_query(query_city_sql))

        # 更新成功，写入log
        if row_count:
            condition_log = 'WHERE ip_range = \'{0}.{1}.x.x\''.format(ia, ib)
            query_sql = r'SELECT * FROM ip_log_info {0};'.format(condition_log)
            rows = self.exec_query(query_sql)
            if len(rows):
                update_log_sql = (r'''UPDATE ip_log_info SET city_count={0}, '''
                                   '''city_finished=\'Y\' {1}''').format(city_count, condition_log)
                update_log_rows = self.exec_no_query(update_log_sql)
                print u'表ip_log_info已更新{0}条记录！'.format(update_log_rows)
            else:
                insert_log_sql = (r'''INSERT INTO ip_log_info('''
                               '''ip_range, city_count, city_finished, '''
                               '''carrier_count, carrier_finished) VALUES '''
                               '''(\'{0}.{1}.x.x\',{2},\'Y\',0,\'N\')''').format(ia, ib, city_count)
                insert_log_rows = self.exec_no_query(insert_log_sql)
                print u'表ip_log_info已插入{0}条记录！'.format(insert_log_rows)

            print u'表ip_city已插入{0}条记录！'.format(row_count)

    def update_carrier(self, ia, ib):
        """
        更新ip_carrier信息
        :param ia: IP的A段地址
        :param ib: IP的B段地址
        :return:
        """

        condition = 'ip like \'{0}.{1}.%\''.format(ia, ib)
        insert_carrier_sql = (r'''INSERT INTO ip_carrier(carrieroperator) '''
                       '''SELECT DISTINCT carrieroperator FROM ip_info '''
                       '''WHERE carrieroperator NOT IN (SELECT carrieroperator FROM ip_carrier) '''
                       '''AND {0};'''.format(condition))
        row_count = self.exec_no_query(insert_carrier_sql)

        query_carrier_sql = (r'''SELECT DISTINCT carrieroperator FROM ip_info '''
                       '''WHERE {0};'''.format(condition))
        rows = self.exec_query(query_carrier_sql)
        carrier_count = len(rows)-1 if ('',) in rows else len(rows)

        # 更新成功，写入log
        if row_count:
            condition_log = 'WHERE ip_range = \'{0}.{1}.x.x\''.format(ia, ib)
            query_sql = r'SELECT * FROM ip_log_info {0};'.format(condition_log)
            rows = self.exec_query(query_sql)
            if len(rows):
                update_log_sql = (r'''UPDATE ip_log_info SET carrier_count={0}, '''
                                   '''carrier_finished=\'Y\' {1}''').format(carrier_count, condition_log)
                update_log_rows = self.exec_no_query(update_log_sql)
                print u'表ip_log_info已更新{0}条记录！'.format(update_log_rows)
            else:
                insert_log_sql = (r'''INSERT INTO ip_log_info('''
                               '''ip_range, city_count, city_finished, '''
                               '''carrier_count, carrier_finished) VALUES '''
                               '''(\'{0}.{1}.x.x\',0,\'N\',{2},\'Y\')''').format(ia, ib, carrier_count)
                insert_log_rows = self.exec_no_query(insert_log_sql)
                print u'表ip_log_info已插入{0}条记录！'.format(insert_log_rows)

            print u'表ip_carrier已插入{0}条记录！'.format(row_count)

    def get_count(self, flag):
        """
        按照IP的AB段分类统计或者统计总数
        :param flag: flag=0表示统计总数; flag=1表示分类统计
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
    ia = 1
    ib = 1
    # updates.update_city(ia, ib)
    updates.update_carrier(ia, ib)


if __name__ == '__main__':

    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}'.format(start_time)
    main()
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print 'Begin:{0}\nEnd:{1}'.format(start_time, end_time)
