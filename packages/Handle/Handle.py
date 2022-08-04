# encodinng = utf-8

"""
    @describe: 数据处理
"""

from packages.conf.dbconf import *
from packages.DB.DBInit import *
from packages.Model.ALS import *

from pyspark.sql import (SparkSession)
import datetime
import pandas as pd
import random
import numpy as np
import math

class Handle(DBInit):

    _MAX_NUM = 20  # 前多少个用户等

    def __init__(self, spark=None):
        if spark is None:
            self._spark = SparkSession.builder.master("yarn").appName("db_handle").getOrCreate()
        else:
            self._spark = spark
        self._ALS = Als()
        self._sc = self._spark.sparkContext
        super(Handle, self).__init__(self._spark)   # 初始化数据库

    def getSpark(self):
        return self._spark

    def RateMoreMovies(self, table="ratings", mode="overwrite"):
        """
        历史热门电影
        :return:
        """
        sql = 'select mid, count(mid) as count from ratings group by mid'
        self._movie_ratings = self.readTable(sql)
        # self._movie_ratings.show(10)
        self.write(self._movie_ratings, "RateMoreMovies", mode=mode)
        del self._movie_ratings

    def RateMoreRecentlyMovies(self, table="ratings", mode="overwrite"):
        """
        最近热门电影
        :return:
        """
        sql = "select mid ,rating_time from ratings order by rating_time DESC"
        self._movie_ratings = self.readTable(sql)
        self._movie_ratings.createOrReplaceTempView("RateMoreRecentlyMovies")
        # recently_date = self._movie_ratings.select("rating_time").rdd.map(lambda x: str(x[0]).split(" ")[0] + " 00:00:00").collect()[0]
        recently_date = self._spark.sql("SELECT rating_time FROM RateMoreRecentlyMovies limit 1").toPandas().iloc[0, 0]
        day = datetime.timedelta(days=-30)  # 最近时间为最近的30天
        recently_date = str(datetime.datetime.strptime(str(recently_date).split(" ")[0] + " 00:00:00", "%Y-%m-%d %H:%M:%S") + day)
        # print("最近日期:", recently_date)
        df = self._spark.sql("SELECT mid, count(mid) as count FROM RateMoreRecentlyMovies  WHERE rating_time >= '{}' group by mid order by count DESC".format(recently_date))
        # df.show(10)
        self.write(df, "RateMoreRecentlyMovies", mode=mode)
        del self._movie_ratings

    def GenresTopMovies(self, table="movies", mode="overwrite"):
        """
        每类别top10电影
        :return:
        """
        sql = "SELECT mid, score, genres FROM movies"
        self._aver = self.readTable(sql)     # 电影均值

        self._genres_rdd = self._sc.parallelize(GENRES)
        # print(self._genres_rdd.cartesian(self._aver.rdd).take(10))
        _MAX_NUM = self._MAX_NUM
        genres_top_movies = self._genres_rdd.cartesian(self._aver.rdd).filter(
            lambda a: a[0].lower() in a[1].genres.lower()
        ).map(
            lambda x: (x[0], (x[1].mid, x[1].score))
        ).groupByKey().map(
            lambda x: (x[0], str(sorted(list(x[1]), key=lambda y: y[1], reverse=True)[:_MAX_NUM]))
        ).toDF(["genres", "recs"])  # type, [ string,string(long, double)]
        # print(genres_top_movies.show())
        self.write(genres_top_movies, "GenresTopMovies", mode=mode)

    def movieUserRecs(self, table="movies", mode="overwrite"):
        """
        电影相似集
        :return:
        """
        sql = "SELECT uid, mid, rating FROM ratings"
        self.train_data_rdd = self.readTable(sql).rdd
        self.train_data_rdd.cache()

        # 获取用户id和电影id
        user_id = self.train_data_rdd.map(
            lambda x: x[0]
        ).distinct()
        movie_id = self.train_data_rdd.map(
            lambda x: x[1]
        ).distinct()
        user_movies = user_id.cartesian(movie_id)   # 空评分矩阵
        rank, iteration, re_ = (72, 5, 0.1)
        self._ALS.train(self.train_data_rdd, rank, iteration, re_)
        pre = self._ALS.predict(user_movies)
        # print(pre.take(10))
        _MAX_NUM = self._MAX_NUM
        userRecs = pre.filter(
                lambda x: x.rating > 0.0).map(
                lambda x: (x.user, (x.product, x.rating))
            ).groupByKey().map(
            lambda x: (x[0], str(sorted(list(x[1]), key=lambda y: y[1], reverse=True)[:_MAX_NUM]))
        ).toDF(["uid", "recs"])
        # print(userRecs.show(10))
        self.write(userRecs, "userRecs", mode=mode)
        del userRecs
        def consinSim(s):
            a = np.array(s[0][1])
            b = np.array(s[1][1])
            return np.dot(a, b) / ((np.linalg.norm(a)) * (np.linalg.norm(b)))
        movie_recs = self._ALS.get_model().productFeatures()
        movie_recs = movie_recs.cartesian(movie_recs).filter(
            lambda x: x[0][0] != x[1][0]
        ).map(
            lambda x: (x[0][0], (x[1][0], consinSim(x)))
        ).filter(
                lambda x: x[1][1] > 0.6
            )
        try:
            movie_recs = movie_recs.groupByKey().map(
                lambda x: (x[0], str(sorted(list(x[1]), key=lambda y: y[1], reverse=True)[:_MAX_NUM]))
            ).toDF(["mid", "recs"])
        except:
            movie_recs = movie_recs.groupByKey().map(
                lambda x: (x[0], str(sorted(list(x[1]), key=lambda y: y[1], reverse=True)[:10]))
            ).toDF(["mid", "recs"])
        self.write(movie_recs, "movieRecs", mode=mode)
        # print(movie_recs.show(10))

    def run(self):
        self.RateMoreMovies()
        self.RateMoreRecentlyMovies()
        self.GenresTopMovies()
        self.movieUserRecs()


    def test(self):
        sql = "SELECT uid, mid, rating FROM ratings"
        data_rdds = self.readTable(sql).rdd.randomSplit([0.8, 0.2], 17)
        train_data_rdd = data_rdds[0]
        test_data_rdd = data_rdds[1]
        param = [[i + 10, 5, 0.1] for i in range(180)]
        old = math.inf
        old_param = []
        rmse_ = []
        for i,j,k in param:
            self._ALS.train(train_data_rdd, iterations=j, rank=i, lambda_=k)
            rmse = self._ALS.adjustModel(test_data_rdd)
            print("rmse为：", rmse)
            rmse_.append(rmse)
            if (old > rmse):
                old = rmse
                old_param=[i, j, k]
        print("rmse变化列表：", rmse_)
        print("参数为{}, {}, {}当前rmse为：{}".format(*old_param, old))