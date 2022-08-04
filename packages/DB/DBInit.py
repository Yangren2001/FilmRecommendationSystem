# encoding=utf-8

"""
    @describe: 数据库初始化
"""

from packages.DB import DB
from packages.conf.dbconf import *


from pyspark.sql import (SparkSession)
from pyspark.sql.types import (TimestampType)
import os
import pandas as pd
import random


class DBInit(DB.DB):

    _base_path = "/".join(os.path.dirname(__file__).split("/")[:-2])

    def __init__(self, spark=None):
        if spark is None:
            self._spark = SparkSession.builder.master("yarn").appName("db_handle").getOrCreate()
        else:
            self._spark = spark
        super(DBInit, self).__init__(self._spark)
        if self.isInit():
            print("初始化数据库")
            os.system("sh {}/bin/init-db.sh {} {}".format(self._base_path, DB_USER, DB_PASSWORD))
            self.LoadData()
            print("完成数据写入！")
            print("初始化完成")

    def isInit(self):
        """
        判断是否初始化
        :return: bool
        """
        file_path = self._base_path + "/src/is_init.config"
        print(file_path)
        with open(file_path, "r+") as f:
            if f.read() == "True":
                f.write("False")
                return True
            else:
                return False

    def LoadData(self):
        """
        初始化表
        :return:
        """
        # dir_file = self._base_path + "/data/datasets/"    # 数据集目录
        files = ["movies.csv", "users.csv", "ratings.csv", "tags.csv"]
        movies_df = self._spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true', delimiter='^').load("/datasets/" + files[0])
        users_df = self._spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("/datasets/" + files[1])
        ratings_df = self._spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("/datasets/" + files[2])
        tags_df = self._spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("/datasets/" + files[3])
        # 解码数据
        # print(users_df.columns)
        self.write(movies_df, files[0].split(".")[0])
        users_df = users_df.rdd.map(lambda x: (x["UserID"], USER_DECODE_SEX[x["Gender"]], USER_DECODE_AGE[str(x["Age"])], USER_DECODE_OCCUPATION[str(x["Occupation"])], str(x["UserID"]))).toDF([DATA_DICT[name] for name in users_df.columns])
        self.write(users_df, files[1].split(".")[0])
        for name in ratings_df.columns:
            ratings_df = ratings_df.withColumnRenamed(name, DATA_DICT[name])
        ratings_df = ratings_df.withColumn("rating_time", ratings_df["rating_time"].cast(TimestampType()))
        self.write(ratings_df, files[2].split(".")[0])
        for name in tags_df.columns:
            tags_df = tags_df.withColumnRenamed(name, DATA_DICT1[name])
        tags_df = tags_df.withColumn("time", tags_df["time"].cast(TimestampType()))
        self.write(tags_df, "tag")

    def test(self):
        pass