import db_handler
import json
import log
import sys
import os
from data_struct import Content
from data_struct import get_content_table_name
from data_struct import construct_content_by_sql_select_res


def load_configs(config_file_path: str):
    with open(config_file_path, "r", encoding="utf8") as f:
        configs = json.load(f)
    return configs


CONFIG_FILE = load_configs("shawn_push_svr_config.json")

pre_sql_dict = {
    # 创建新的内容库table
    "create_content_table": "CREATE TABLE @table_name (\
                                id int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id', \
                                content_id varchar(255) NOT NULL UNIQUE COMMENT '*内容id', \
                                title varchar(255) NOT NULL COMMENT '*内容标题', \
                                create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '消息创建时间', \
                                origin varchar(225) NOT NULL COMMENT '*消息来源', \
                                emergency_flag tinyint(4) unsigned COMMENT '是否为紧急数据', \
                                security_level tinyint(4) unsigned COMMENT '消息的保密级别', \
                                info varchar(255) NOT NULL COMMENT '内容摘要', \
                                content_bs64 TEXT(65535) COMMENT '*消息内容的base64存储', \
                                PRIMARY KEY(id) \
                            ) COMMENT='内容库'; ",

    # 添加内容至内容库
    "insert_content_to_db": "INSERT INTO @table_name (content_id, title, origin, emergency_flag, security_level, \
                                                      info, content_bs64) VALUES (%s, %s, %s, %s, %s, %s, %s);",

    # 通过content_id批量获取Content
    "get_content_by_id": "SELECT * FROM @table_name WHERE content_id IN (@@content_id_list);"
}


def get_list_str_params_sql(param_list):
    sql_str = ""
    for param in param_list:
        sql_str += '"{}"'.format(param) + ", "
    return sql_str[:-2]


def get_list_int_params_sql(param_list):
    sql_str = ""
    for param in param_list:
        sql_str += '{}'.format(param) + ", "
    return sql_str[:-2]


class DbProxyHelper:
    def __init__(self):
        self.logger = log.Logger("db_proxy_helper_log", CONFIG_FILE["log_configs"]["log_path_win"],
                                 CONFIG_FILE["log_configs"]["log_level"]).start_log()
        # 内容库数据库连接对象
        self.db_content_handler = db_handler.MysqlHandler(CONFIG_FILE["db_configs"]["db_content_connection_config"],
                                                          self.logger)
        self.db_content_handler.connect()

    def insert_content(self, content_data: Content):
        self.logger.debug(__name__, "insert_content called")
        # 创建content_id
        content_data.create_content_id(content_data.origin)
        if not content_data.check():
            self.logger.error(__name__, "Content check failed: {}".format(content_data.string()))
            return False
        # 得到该数据的分表情况
        table_name = content_data.get_table_name()
        if not table_name:
            self.logger.error(__name__, "Table name error, may content_id failed format check, content {}".format(
                content_data.string()))
            return False
        if not self.db_content_handler.check_table_exists(table_name):
            self.db_content_handler.query(pre_sql_dict["create_content_table"].replace("@table_name", table_name))
        val = (content_data.content_id, content_data.title, content_data.origin, content_data.emergency_flag,
               content_data.security_level, content_data.info, content_data.content_bs64)
        return self.db_content_handler.insert_data(
            pre_sql_dict["insert_content_to_db"].replace("@table_name", table_name), val)

    # 通过content_id批量获取Content
    def batch_get_content_by_id(self, content_id_list: list):
        self.logger.debug(__name__, "batch_get_content_by_id called")
        content_id_map = dict()
        for content_id in content_id_list:
            # 得到该数据的分表情况
            table_name = get_content_table_name(content_id)
            if not table_name:
                self.logger.error(__name__, "Table name error, may content_id failed format check. content_id: {}"
                                  .format(content_id))
                continue
            if table_name in content_id_map:
                content_id_map[table_name].append(content_id)
            else:
                content_id_map[table_name] = list()
                content_id_map[table_name].append(content_id)
        # 遍历所有数据
        content_list = list()
        for table_name, content_id_list_ in content_id_map.items():
            # 将content_id构建为sql语句并查询
            res = self.db_content_handler.select(pre_sql_dict["get_content_by_id"].replace("@table_name", table_name)
                                                 .replace("@@content_id_list",
                                                          get_list_str_params_sql(content_id_list_)))
            content_list += construct_content_by_sql_select_res(res)
        return content_list
