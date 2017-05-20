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
        :param sql: 要执行的SQL语句
        :return:
        """

        MysqlDB.exec_no_query(self, sql)

    def query_sql(self, sql):
        """
        执行select查询语句，返回结果集
        :param sql: 要执行的SQL语句
        :return:
        """

        rows = MysqlDB.exec_query(self, sql)
        return rows

    def get_ip_by_condition(self, condition):
        """
        通过条件查询IP信息
        :param condition: 查询条件
        :return:
        """

        sql = "SELECT * FROM ip_info where 1=1 {condition};".format(condition=condition)
        rows = self.exec_query(sql)
        return rows
