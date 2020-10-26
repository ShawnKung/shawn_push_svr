# from demo.db_create_insert_select import main
# main()

import utils
import time
from db_proxy_svr.db_proxy_helper import DbProxyHelper
from data_struct import Content
content = Content()
content.content_id = utils.get_timestamp()
content.create_time = utils.get_timestamp()
content.emergency_flag = False
content.origin = "test_data"
content.level = 0
db_proxy_helper_ = DbProxyHelper()
db_proxy_helper_.insert_content(content)

