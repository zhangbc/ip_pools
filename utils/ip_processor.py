# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
IP信息处理类
author:         zhangbc
create_time:    2017-05-20
"""


from db_mysql import MysqlDB


class IpProcessor(MysqlDB):
    """
    IP信息数据库相关操作处理类，继承父类MysqlDB
    """

    def __init__(self):
        MysqlDB.__init__(self)

    def get_total_ips(self):
        """
        统计已入库的IP总数
        :return:
        """

        query_sql = 'SELECT COUNT(1) counts FROM ip_info;'
        result = self.exec_query(query_sql)
        return result[0][0]

    def get_count_by_group(self):
        """
        按照IP的AB段分类统计
        :return:
        """

        query_sql = 'SELECT SUBSTRING_INDEX(ip,\'.\',2) ips, COUNT(1) counts ' \
                    'FROM ip_info GROUP BY SUBSTRING_INDEX(ip,\'.\',2);'
        result = self.exec_query(query_sql)
        return result

    def update_sql(self, sql):
        """
        执行查询语句，如Create,Insert,Delete,update,drop等。
        :param sql:
        :return:
        """

        MysqlDB.exec_no_query(self, sql)

    def query_sql(self, sql):
        """
        执行select查询语句，返回结果集
        :param sql:
        :return:
        """

        rows = MysqlDB.exec_query(self, sql)
        return rows

    def get_ip_by_condition(self, columns, condition):
        """
        通过条件查询IP信息
        :param columns:
        :param condition: 查询条件, 如：‘and ip like ‘1.1.%’’
        :return:
        """

        sql = "SELECT {columns} FROM ip_info where 1=1 {condition};"\
            .format(columns=columns, condition=condition)
        rows = self.exec_query(sql)
        return rows

    @staticmethod
    def get_ips(ia, ib, ic, ie):
        """
        获取批量IP地址
        :param ia: IP地址的A段, 若为0，则1~255
        :param ib: IP地址的B段, 若为0，则0~255
        :param ic: IP地址的C段, 若为0，则0~255
        :param ie: IP地址的D段, 若为0，则0~255
        :return:
        """

        radix = xrange(0, 256)
        if ia > 0:
            ia = xrange(ia, ia+1)
        else:
            ia = [i for i in radix if i > 0]

        if ib > 0:
            ib = xrange(ib, ib+1)
        else:
            ib = radix

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
