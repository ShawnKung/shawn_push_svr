import db_handler
import json
import log
from data_struct import Content


def load_configs(config_file_path: str):
    with open(config_file_path, "r", encoding="utf8") as f:
        configs = json.load(f)
    return configs


CONFIG_FILE = load_configs("shawn_push_svr_config.json")


pre_sql_dict = {
    "create_content_table": "CREATE TABLE @table_name (\
                                id int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id', \
                                content_id varchar(255) NOT NULL UNIQUE COMMENT '内容id', \
                                create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '消息创建时间', \
                                origin varchar(225) NOT NULL COMMENT '消息来源', \
                                emergency_flag tinyint(4) unsigned COMMENT '是否为紧急数据', \
                                security_level tinyint(4) unsigned COMMENT '消息的保密级别', \
                                content_bs64 varchar(255) COMMENT '消息内容的base64存储', \
                                PRIMARY KEY(id) \
                            ) COMMENT='内容库'; ",
    "insert_content_to_db": "INSERT INTO @table_name (content_id, origin, emergency_flag, security_level, content_bs64) VALUES (%s, %s, %s, %s, %s)"
}


class DbProxyHelper:
    def __init__(self):
        logger = log.Logger("db_proxy_helper_log", CONFIG_FILE["log_configs"]["log_path_win"],
                            CONFIG_FILE["log_configs"]["log_level"]).start_log()
        # 内容库数据库连接对象
        self.db_content_handler = db_handler.MysqlHandler(CONFIG_FILE["db_configs"]["db_content_connection_config"], logger)
        self.db_content_handler.connect()

    def insert_content(self, content_data: Content):
        # 创建content_id
        content_data.create_content_id(content_data.origin)
        # 得到该数据的分表情况
        table_name = content_data.get_table_name()
        if not self.db_content_handler.check_table_exists(table_name):
            self.db_content_handler.query(pre_sql_dict["create_content_table"].replace("@table_name", table_name))
        val = (content_data.content_id, content_data.origin, content_data.emergency_flag, content_data.security_level, content_data.content_bs64)
        return self.db_content_handler.insert_data(pre_sql_dict["insert_content_to_db"].replace("@table_name", table_name), val)


