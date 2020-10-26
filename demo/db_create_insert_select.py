from db_proxy_svr.db_handler import MysqlHandler
import log
config_db_id_map = {
    "user": "root",
    "passwd": "m8H1n6Q2C59Y",
    "host": "127.0.0.1",
    "port": 3306,
    "db": "test"
}

TABLE_NAME = "customers"

create_table_query = "CREATE TABLE {}(\
id INT AUTO_INCREMENT PRIMARY KEY, \
name VARCHAR(255) UNIQUE , \
address VARCHAR(255), \
sex VARCHAR(225) , \
age INT(10) , \
sl INT(10))".format(TABLE_NAME)


def main():
    logger = log.Logger("db_win_demo", log_level="DEBUG").start_log()
    sql_handler = MysqlHandler(config_db_id_map, logger)
    sql_handler.connect()
    if not sql_handler.check_table_exists(TABLE_NAME):
        sql_handler.query(create_table_query)

    sql = "INSERT INTO {} (name,address,sex,age,sl) VALUES (%s,%s,%s,%s,%s)".format(TABLE_NAME)

    val = ("John", "Highway 21", "M", 23, 5000)

    vals = [("Tom", "ABC 35", "M", 35, 14000),
            ("Tom1", "Highway 29", "M", 28, 6700),
            ("Lily", "Road 11", "F", 30, 8000),
            ("Martin", "Road 24", "M", 35, 14000),
            ("Sally", "Fast 56", "M", 32, 15000)]

    sql_handler.insert_data(sql, val)
    sql_handler.insert_data(sql, vals)

    res = sql_handler.select_from_table(TABLE_NAME)
    print(res["name"])
    print(res["data"])

