import logging
import os
import pymysql
from dbutils.pooled_db import PooledDB
from pymysql.cursors import DictCursor
import configparser

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Config(configparser.ConfigParser):
    """
    # Config().get_content("user_information")
    配置文件里面的参数
    [MYSQL]
    host = 127.0.0.1
    port = 3306
    user = root
    password = 123456
    """

    def __init__(self, config_filepath):
        super(Config, self).__init__()
        self.read(config_filepath)

    def get_sections(self):
        return self.sections()

    def get_options(self, section):
        return self.options(section)

    def get_content(self, section):
        result = {}
        for option in self.get_options(section):
            value = self.get(section, option)
            result[option] = int(value) if value.isdigit() else value
        return result

    def optionxform(self, optionstr):
        return optionstr


class DBUtil:
    # 连接池对象
    __pool = None
    # 初始化
    def __init__(self, host, user, password, database, port=3306, charset="utf8"):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = str(password)
        self.database = database
        self.charset = charset
        self.conn = self.__get_connection()
        self.cursor = self.conn.cursor()

    def __get_connection(self):
        if DBUtil.__pool is None:
            __pool = PooledDB(
                creator=pymysql,
                mincached=1,
                maxcached=30,
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                db=self.database,
                use_unicode=False,
                charset=self.charset,
            )
        return __pool.connection()

    def begin(self):
        """
        开启事务
        """
        self.conn.autocommit(0)

    def end(self, option="commit"):
        """
        结束事务
        """
        if option == "commit":
            self.conn.commit()
        else:
            self.conn.rollback()

    def connect(self):
        """
        直接连接数据库
        :return conn: pymysql连接
        """
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                use_unicode=True,
                charset=self.charset,
            )
            self.conn = conn
            self.cursor = conn.cursor()
            return conn
        except Exception as e:
            logger.error("Error connecting to mysql server.")
            raise

    # 关闭数据库连接
    def close(self):
        try:
            if self.conn is not None:
                self.cursor.close()
                self.conn.close()
        except Exception as e:
            logger.error("Error closing connection to mysql server")

    def get_one(self, sql):
        """查询操作，查询单条数据"""
        # res = None
        try:
            # self.connect()
            self.conn = self.__get_connection()
            self.cursor = self.conn.cursor()
            self.cursor.execute(sql)
            return self.cursor.fetchone()
        except Exception as e:
            raise
        finally:
            self.close()

    def get_all(self, sql):
        """查询操作，查询多条数据"""
        try:
            # self.connect()
            self.conn = self.__get_connection()
            self.cursor = self.conn.cursor()
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            raise
        finally:
            self.close()

    def get_all_obj(self, sql, table_name, *args):
        """查询数据库对象"""
        resList = []
        fields_list = []
        try:
            if len(args) > 0:
                fields_list.extend(iter(args))
            else:
                fields_sql = (
                    "select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' and table_schema = '%s'"
                    % (table_name, self.conn_name)
                )
                fields = self.get_all(fields_sql)
                fields_list.extend(item[0] for item in fields)
            # 执行查询数据sql
            res = self.get_all(sql)
            for item in res:
                obj = {fields_list[count]: x for count, x in enumerate(item)}
                resList.append(obj)
            return resList
        except Exception as e:
            raise

    def insert(self, sql, params=None):
        """
        插入操作
        :return count: 影响的行数
        """
        return self.__edit(sql, params)

    def update(self, sql, params=None):
        """
        更新操作
        :return count: 影响的行数
        """
        return self.__edit(sql, params)

    def delete(self, sql, params=None):
        """
        删除操作
        :return count: 影响的行数
        """
        return self.__edit(sql, params)

    def __edit(self, sql, params=None):
        max_attempts = 3
        attempt = 0
        count = 0
        while attempt < max_attempts:
            try:
                self.conn = self.__get_connection()
                self.cursor: DictCursor = self.conn.cursor()
                if params is None:
                    count = self.cursor.execute(sql)
                else:
                    count = self.cursor.execute(sql, params)
                self.conn.commit()
            except Exception as e:
                logger.error(e)
                self.conn.rollback()
                count = 0
            finally:
                self.close()
        return count

    def execute(self, sql, params=None):
        """执行单条sql"""
        max_attempts = 3
        attempt = 0
        while attempt < max_attempts:
            try:
                self.conn = self.__get_connection()
                self.cursor = self.conn.cursor()
                if params is None:
                    result = self.cursor.execute(sql)
                else:
                    result = self.cursor.execute(sql, params)
                self.conn.commit()
                return result
            except pymysql.err.ProgrammingError as err:
                logger.exception(err)
                break
            except Exception as e:
                attempt += 1
                logger.warning(f"retry: {attempt}")
                logger.exception(e)
            finally:
                self.close()

    def truncate(self, table: str):
        """清空表
        :param table: 表名
        """
        sql = f"truncate table {table}"
        self.execute(sql)

    def executemany(self, sql, data):
        """批量执行sql"""
        max_attempts = 3
        attempt = 0
        while attempt < max_attempts:
            try:
                self.conn = self.__get_connection()
                self.cursor = self.conn.cursor()
                result = self.cursor.executemany(sql, data)
                self.conn.commit()
                return result
            except pymysql.err.ProgrammingError as err:
                logger.exception(err)
                break
            except Exception as e:
                attempt += 1
                logger.warning(f"retry: {attempt}")
                logger.exception(e)
            finally:
                self.close()


# config = {
#     "HOST": "www.csubigdata.com",
#     "USER": "root",
#     "PASSWORD": "Bigdata.db@2021",
#     "DATABASE": "stoploss",
#     "PORT": 3306,
#     "CHARSET": "utf8",
# }
config_filepath = f"{os.path.dirname(os.path.dirname(__file__))}/config/dev.ini"

config = Config(config_filepath).get_content("MYSQL")
conn = DBUtil(
    host=config["HOST"],
    port=int(config["PORT"]),
    database=config["DATABASE"],
    user=config["USER"],
    password=config["PASSWORD"],
    charset=config["CHARSET"],
)


def get_connection():
    return conn


if __name__ == "__main__":
    db = get_connection()
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute("SELECT VERSION()")

    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchone()

    print(f"Database version : {data} ")

    # 关闭数据库连接
    db.close()
