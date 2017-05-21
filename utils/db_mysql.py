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


class MysqlDB(object):
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
        执行select查询语句，返回结果集
        :param sql: 要执行的SQL语句
        :return:
        """

        cur = self.__connect__()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except cymysql.MySQLError, ex:
            print 'MySQL.Error :%s \n%s' % (str(ex[0]), str(ex[1]))
            sys.exit()
        finally:
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
        row_count = 0  # 更新/插入受影响的行数
        try:
            cur.execute(sql)
            self.conn.commit()
            row_count = cur.rowcount
        except cymysql.MySQLError, ex:
            print 'MySQL.Error :%s %s' % (str(ex[0]), str(ex[1]))
            self.conn.rollback()
        finally:
            cur.close()
            self.conn.close()

        return row_count
