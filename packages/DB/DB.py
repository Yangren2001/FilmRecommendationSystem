# encoding=utf-8

"""
    @describe: 这是一个数据库操作库
"""

from pyspark.sql import (SparkSession)
from packages.conf.dbconf import *
from pyspark import (SparkConf, SparkContext)
import pymysql

class DB:
    _spark = None   # spark session对象
    _properties = {
        "user": DB_USER,
        "password": DB_PASSWORD
    }
    _con_db = None
    _cursor = None

    def __init__(self, spark=None):
        if spark is None:
            self._spark = SparkSession.builder.master("yarn").appName("db_handle").getOrCreate()
        else:
            self._spark = spark

    def connect(self, host=None, db=None, user=None, password=None):
        """
        pymysql连接数据库
        :param host:
        :param db:
        :param user:
        :param password:
        :return:
        """
        if host is None:
            host = DB_HOST
        if db is None:
            db = DB_DATABASE
        if user is None:
            user = DB_USER
        if password is None:
            password = DB_PASSWORD
        self._con_db = pymysql.connect(
            host=host,
            database=db,
            user=user,
            password=password,
            charset="utf8"
        )
        self._cursor = self._con_db.cursor()

    def setProperties(self, user, pwd):
        """
        设置用户和密码
        :param user:
        :param pwd:
        :return:
        """
        self._properties = {
            "user": user,
            "password": pwd
        }

    def sql(self, sql):
        """
        执行sql语句不包括查询
        :param sql:
        :return:
        """
        try:
            self._cursor.execute(sql)
            self._con_db.commit()
            return True
        except Exception as e:
            print(e)
            print("数据库操作失败，语句{}".format(sql))
            self._con_db.rollback()
            return False

    def select(self, sql):
        """
        数据库查询操作
        :param sql:
        :param cols_name:
        :return: df
        """
        try:
            c = self._cursor.execute(sql)
            if c == 0:
                return None
            data = list(self._cursor.fetchall())
            return data
        except:
            print("Error: unable to fetch data")
            return None

    def read(self, table, url=None, properties=None):
        """
        读取数据库数据
        :param table:表名 str
        :param url: 数据库地址
        :param properties:dict user password
        :return: DateFrame
        """
        if url is None:
            url = DB_URL
        if properties is None:
            properties = self._properties
        return self._spark.read.jdbc(url=url, table=table, properties=properties)

    def write(self, df,  table, url=None, properties=None, mode="append"):
        """
        写入数据库表数据
        :param df: 数据 DateFrame
        :param table:表名 str
        :param url: 数据库地址
        :param properties:dict user password
        :param mode: error	原表存在则报错
                    ignore	原表存在，不报错且不写入数据
                    append	默认值，新数据在原表行末追加
                    overwrite	覆盖原表
        :return: None
        """
        if url is None:
            url = DB_URL
        if properties is None:
            properties = self._properties
        df.write.jdbc(url=url, mode=mode, table=table, properties=properties)

    def readTable(self, sql):
        """
        读取指定字段数据
        :param sql: sql语句
        :return: DateFrame
        """
        return self._spark.read.format("jdbc")\
            .option("url", DB_URL)\
            .option("query", sql)\
            .load()

    def close(self):
        self._con_db.close()

    def test(self):
        pass
