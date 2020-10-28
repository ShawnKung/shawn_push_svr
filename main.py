# from demo.db_create_insert_select import main
# main()

import utils
import time
from db_proxy_svr.db_proxy_helper import DbProxyHelper
from data_struct import Content
content = Content()
content.content_id = utils.get_timestamp()
content.title = "测试信息" + utils.get_time_str()
content.create_time = utils.get_timestamp()
content.emergency_flag = False
content.origin = "test_data"
content.level = 0
content.info = "我是摘要"
content.content_bs64 = "TEST"
db_proxy_helper_ = DbProxyHelper()
db_proxy_helper_.insert_content(content)
res = db_proxy_helper_.batch_get_content_by_id(["test_data_1603892587002332929", "0"])
time.sleep(0.1)
for content in res:
    print(content.string())
