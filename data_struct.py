import utils
from random import randint
import re


def check_content_id(content_id: str):
    return True if re.match("^[a-zA-Z_]*[0-9]{19}$", content_id) else False


# 根据content_id得到数据分表
def get_content_table_name(content_id: str):
    if not check_content_id(content_id):
        return ""
    # 得到秒数
    year, week_num, _ = utils.get_week(content_id[:-9].split("_")[-1])
    return "content_table_{}_{}".format(year, week_num)


# 根据MysqlHandler.select()返回的结果构建Content列表
def construct_content_by_sql_select_res(sql_res):
    name_2_index_dict = dict()
    content_list = list()
    # 构建数据库列名->列下标字典
    for index, name in enumerate(sql_res["name"]):
        name_2_index_dict[name] = index
    for data in sql_res["data"]:
        content = Content()
        content.content_id = data[name_2_index_dict["content_id"]]
        content.title = data[name_2_index_dict["title"]]
        content.create_time = data[name_2_index_dict["create_time"]]
        content.origin = data[name_2_index_dict["origin"]]
        content.emergency_flag = data[name_2_index_dict["emergency_flag"]]
        content.security_level = data[name_2_index_dict["security_level"]]
        content.info = data[name_2_index_dict["info"]]
        content.content_bs64 = data[name_2_index_dict["content_bs64"]]
        content_list.append(content)
    return content_list


class Content:
    # 内容id（时间戳），创建时间，来源，是否为紧急消息，保密级别
    def __init__(self):
        # *为必填项
        # *内容id（{来源}_{time.time()时间戳去掉小数点}+3位随机数防止碰撞）
        self.content_id: str = ""
        # *内容标题
        self.title: str = ""
        # 加到数据库中的创建时间
        self.create_time: str = ""
        # *数据来源
        self.origin: str = ""
        # 是否为紧急信息
        self.emergency_flag: bool = False
        # 信息的保密级别
        self.security_level: int = 0
        # 内容摘要，长度较短
        self.info: str = ""
        # *信息的字符串base64
        self.content_bs64: str = ""

    def create_content_id(self, origin: str):
        now = utils.get_timestamp()
        second, millisecond = str(now).split(".")[:]
        self.content_id = origin + "_" + str(second) + str(millisecond)[:6].rjust(6, "0") + str(randint(100, 999))

    def get_table_name(self):
        return get_content_table_name(self.content_id)

    def string(self, length=120):
        info = "content_id:" + self.content_id + ", title:" + self.title + ", origin:" + self.origin + ", info:" \
               + self.info
        return info[:length]

    def check(self):
        if check_content_id(self.content_id) and self.origin and self.content_bs64 and self.title:
            return True
        return False


