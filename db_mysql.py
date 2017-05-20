# /usr/local/python2.7.11/bin/python
# coding: utf-8

"""
mysql通用类
author:         zhangbc
create_time:    2017-05-19
"""

import sys
import cymysql
import settings


class MysqlDB:
    """
    操作MySQL的类
    """

    def __init__(self):
        self.args_db = self.from_settings()

    @classmethod
    def from_settings(cls):
        """
        获取配置信息
        :return:
        """

        args_db = dict(
            host=settings.MYSQL_HOST,
            db=settings.MYSQL_DBNAME,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PASSWD,
            charset=settings.MYSQL_CHARSET,
            port=settings.MYSQL_PORT
        )
        return args_db

    def __connect__(self):
        """
        利用配置信息连接MySQL
        :return:
        """
        try:
            kwargs = self.args_db
            self.conn = cymysql.connect(**kwargs)
            cur = self.conn.cursor()
        except cymysql.MySQLError as ex:
            print 'MySQL connecting error,reason is:'+str(ex[1])
            sys.exit()

        return cur

    def exec_query(self, sql):
        """
        执行查询语句，返回结果集
        :param sql: 要执行的SQL语句
        :return:
        """

        cur = self.__connect__()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except cymysql.MySQLError, ex:
            print 'MySQL.Error :%s \ns' % (str(ex[0]), str(ex[1]))
            sys.exit()

        cur.close()
        self.conn.close()
        return rows

    def exec_no_query(self, sql):
        """
        执行查询语句，如Create,Insert,Delete,update,drop等。
        :param sql: 要执行的SQL语句
        :return:
        """

        cur = self.__connect__()
        try:
            cur.execute(sql)
            self.conn.commit()
        except cymysql.MySQLError, ex:
            print 'MySQL.Error :%s %s' % (str(ex[0]), str(ex[1]))
            sys.exit()
        cur.close()
        self.conn.close()

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
