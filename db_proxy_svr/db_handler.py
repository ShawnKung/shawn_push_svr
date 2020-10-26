# -*- coding: utf-8 -*-

"""
Created Date: 2020-10-26 11:39:12
Author: yuxianggong (yuxianggong@tencent.com>)
Desc:
    该文件为Windows/Linux平台提供数据接入接口功能
    Windows新机器部署依赖安装：
    pip install mysql
    Linux平台部署安装：
    CentOS7:
    yum install python-pip, python-devel
    pip install mysql-python
-----
Last Modified:
Modified By:
-----
"""

import utils


# 定义未连接数据库异常
class ConnectError(Exception):
    pass


class MysqlHandler:
    def __init__(self, config, logger):
        self.config = config
        self.con = None  # 必须有，不然会con会被释放，从而cursor无法使用
        self.cur = None
        self.fetchall_res = None
        self.logger = logger
        # 用来存放垃圾信息

    def __connect_check__(self):
        if not self.con:
            raise ConnectError("Did not connect to db. Need to do MysqlHandler.connect(config) first")

    def __del__(self):
        self.disconnect()

    def __atom_query__(self, sql_query):
        self.__connect_check__()
        # 原子性
        try:
            # 执行sql语句
            self.cur.execute(sql_query)
            try:
                self.fetchall_res = self.cur.fetchall()
            except Exception as e:
                pass
            # 提交到数据库执行
            self.con.commit()
            self.logger.debug("Query success: {} ".format(sql_query))
            return True
        except Exception as e:
            # 发生错误时回滚
            self.con.rollback()
            self.logger.error("Query error: {} {} ".format(e, sql_query))
            return False

    def connect(self):
        if "Windows" == utils.get_platform():
            import mysql.connector
            connect_func = mysql.connector.Connect
        elif "Linux" == utils.get_platform():
            import MySQLdb
            connect_func = MySQLdb.connect
        else:
            self.logger.error("Platform is {}, not supported".format(utils.get_platform()))
            return False
        try:
            self.con = connect_func(**self.config)
            self.logger.debug("connect database success : {}".format(self.config))
        except Exception as e:
            self.logger.error("connect database fails! config:{} , {}".format(e, self.config))
        if not self.con:
            return False
        self.cur = self.con.cursor()
        return True

    def disconnect(self):
        if self.con:
            self.logger.debug("Disconnect mysql : {}".format(self.config))
            self.con.close()
            self.con = None

    def check_table_exists(self, table_name: str):
        self.__connect_check__()
        stmt = "SHOW TABLES LIKE '" + table_name + "'"
        self.cur.execute(stmt)
        return self.cur.fetchone()

    def insert_data(self, sql, val):
        self.__connect_check__()
        if isinstance(val, tuple):
            exe_sql = self.cur.execute
        elif isinstance(val, list):
            exe_sql = self.cur.executemany
        else:
            self.logger.error("Type of val error, should be tuple or list")
            return False
        try:
            exe_sql(sql, val)
            self.con.commit()  # 必须提交才生效
            self.logger.debug("insert data success: {} {}".format(sql, val))
            return True
        except Exception as e:
            self.con.rollback()
            self.logger.error("insert data error {} : {} {}".format(e, sql, val))
            return False

    def select_from_table(self, table_name, **kwargs):
        self.__connect_check__()
        """
        SELECT * FROM table (WHERE ..) (LIMIT ..)
        """
        sql_query = "SELECT * FROM {}".format(table_name)
        if "where" in kwargs:
            sql_query += " WHERE {}".format(kwargs["where"])
        if "offset" in kwargs and not "count" in kwargs:
            self.logger.error("Missing \"count\", while \"offset\"={}".format(kwargs["offset"]))
            return None
        elif "count" in kwargs and "offset" in kwargs:
            assert int(kwargs["count"]) > 0
            assert int(kwargs["offset"]) >= 0
            sql_query += " LIMIT {}, {}".format(kwargs["offset"], kwargs["count"])
        elif "count" in kwargs:
            assert int(kwargs["count"]) > 0
            sql_query += " LIMIT {}".format(kwargs["count"])

        sql_query += ";"
        return self.select(sql_query)

    def update_data(self, table_name, set_query, where_query):
        sql_query = 'UPDATE {} SET {} WHERE {};'.format(table_name, set_query, where_query)
        try:
            self.__atom_query__(sql_query)
            return True
        except Exception as e:
            self.con.rollback()
            self.logger.error("UPDATE data error {} : {}".format(e, sql_query))
            return False

    def select(self, query_string):
        self.__connect_check__()
        ret = self.__atom_query__(query_string)
        result = dict()
        result["name"] = list()
        result["data"] = tuple()
        if ret:
            # 字段名称列表
            result["name"] = [i[0] for i in self.cur.description]
            # 结果集，( (v11,v12,v13...), (v21, v22, v23...) ... )
            result["data"] = self.fetchall_res
        return result

    def query(self, query):
        self.__connect_check__()
        self.__atom_query__(query)
