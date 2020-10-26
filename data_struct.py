import utils
from random import randint


class Content:
    # 内容id（时间戳），创建时间，来源，是否为紧急消息，保密级别
    def __init__(self):
        # 内容id（{来源}_{time.time()时间戳去掉小数点}+3位随机数防止碰撞）
        self.content_id: int = 0
        # 加到数据库中的创建时间
        self.create_time: str = ""
        # 数据来源
        self.origin: str = ""
        # 是否为紧急信息
        self.emergency_flag: bool = False
        # 信息的保密级别
        self.security_level: int = 0
        # 信息的字符串base64
        self.content_bs64: str = ""

    def create_content_id(self, origin: str):
        now = utils.get_timestamp()
        second, millisecond = str(now).split(".")[:]
        self.content_id = origin + "_" + str(second) + str(millisecond)[:6].rjust(6, "0") + str(randint(100, 999))

    def get_table_name(self):
        # 得到秒数
        year, week_num, _ = utils.get_week(self.content_id[:-9].split("_")[-1])
        return "{}_{}".format(year, week_num)
