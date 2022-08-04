# encodinng = utf-8

"""
    @describe: 实时处理
"""

from pyspark import (SparkConf, SparkContext)
from pyspark.streaming import (StreamingContext, DStream)
from pyspark.sql import (SparkSession)
import os
import pymysql
import math
import datetime

from packages.conf.realtimeconf import *
from packages.DB.DB import *

class RealTimeHandle:
    _sc = None
    _spark = None
    _ssc = None
    _conf = None
    _db = None
    _sk_flag = True

    def __init__(self, sc=None, topic="test"):
        if sc is None:
            self._conf = SparkConf().setAppName("stream").setMaster("yarn")
            self._sc = SparkContext(conf=self._conf)
        else:
            self._sc = sc
            self._conf = self._sc.getConf()

        self._spark = SparkSession(self._sc)
        self._ssc = StreamingContext(self._sc, INTERVAL)
        self._db = DB(self._spark)

    def receive(self, path="hdfs://hadoop103:8020/film/logs/*"):
        """
        数据输入
        :param path:
        :return:
        """
        return self._ssc.textFileStream(path).flatMap(
            lambda x: x.split("$")
        ).filter(
            lambda x: "�" not in x
        ).filter(
            lambda x: x != ""
        ).filter(
            lambda x: "apache.hadoop.io.LongWritable" not in x
        ).filter(
            lambda x: "|" in x
        )

    def loadMovieRecs(self):
        """
        加载电影矩阵
        :return: rdd(mid, smid, score)
        """
        sql = "SELECT * FROM movieRecs"
        return self._db.readTable(sql).rdd.map(
            lambda x: [x[0], eval(x[1])]
        ).collectAsMap()

    def realHandle(self):
        """
        实时处理

        :return:
        """
        movieResc = self.loadMovieRecs()
        simMoviesMatrixBroadCast = self._sc.broadcast(movieResc)
        # compute = self.computeMovieScores

        def movieTransform(movie_name):
            """
            电影名转换id
            :param movie_name:
            :return:
            """
            _con_db = pymysql.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            _cursor = _con_db.cursor()
            sql = "SELECT mid FROM movies WHERE name='{}'".format(movie_name)
            try:
                c = _cursor.execute(sql)
                if c == 0:
                    _con_db.close()
                    return "-1"
                data = list(_cursor.fetchall())[0][0]
                # print(data)
                _con_db.close()
                return data
            except:
                print("Error: unable to fetch data")
                _con_db.close()
                return "-1"

        def isRatingExist(uid, mid):
            """

            查询用户是否看过
            :param uid:
            :param mid:
            :return: bool
            """
            _con_db = pymysql.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            _cursor = _con_db.cursor()
            sql = "SELECT * FROM ratings WHERE uid={} AND mid={}".format(uid, mid)
            try:
                c = _cursor.execute(sql)
                _con_db.close()
                if c == 0:
                    return False
                else:
                    return True
            except:
                print("Error: unable to fetch data")
                _con_db.close()
                return None

        def getUserRecsRating(uid, t1, t2=30):
            """
            获取用户最近评分
            :param uid:
            :param t1:
            :param t2: 最近天数
            :return: rdd 最近评分
            """
            _con_db = pymysql.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            _cursor = _con_db.cursor()
            day = datetime.timedelta(days=-t2)
            date = datetime.datetime.fromtimestamp(eval(t1)) + day
            date = str(date).split(" ")[0] + " 00:00:00"
            sql = "SELECT * FROM ratings WHERE uid={} AND rating_time >= '{}'".format(uid, date)
            try:
                c = _cursor.execute(sql)
                if c == 0:
                    sql1 = "SELECT * FROM ratings WHERE uid={} limit 30".format(uid)
                    c = _cursor.execute(sql1)
                    if c == 0:
                        _con_db.close()
                        return None
                data = list(_cursor.fetchall())
                # print(data)
                _con_db.close()
                data = [(i[1], i[2]) for i in data]
                return data
            except:
                print("Error: unable to fetch data")
                _con_db.close()
                return None

        def getMovieRecs(uid, mid, smid, simMovies):
            """
            获取用户当前评分电影相似集合
            :param uid:
            :param mid:
            :param smid:
            :param simMovies: 相似矩阵
            :return:
            """
            # print(simMovies.keys())
            # if mid not in list(simMovies.keys()):
            #     return 0.0
            r = None
            try:
                r = [i for i in simMovies[mid] if i[0] == smid and isRatingExist(uid, i[0])]
            except:
                return 0.0
            if len(r) == 0:
                return 0.0
            else:
                return r[0][1]

        def computeMovieScores(uid, mid, t, simMovies):
            """
            计算电影评分
            :param uid:
            :param t:
            :param mid: 用户当前评分电影
            :param simMovies:
            :return:
            """
            user_rating = getUserRecsRating(uid, t)
            movies_recs = None
            try:
                movies_recs = simMovies[mid]  # 观看电影相似列表
            except:
                sql1 = "SELECT recs FROM userRecs WHERE uid={}".format(uid)
                _con_db = pymysql.connect(
                    host=DB_HOST,
                    database=DB_DATABASE,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                _cursor = _con_db.cursor()
                try:
                    c = _cursor.execute(sql1)
                    if c == 0:
                        _con_db.close()
                        return []
                    data = list(_cursor.fetchall())[0][0]
                    data = eval(data)
                    # print(data)
                    _con_db.close()
                    movies_recs = [(i[0], i[1]) for i in data]
                except:
                    print("Error: unable to fetch data")
                    _con_db.close()
            if user_rating is None:
                return movies_recs
            recs_ls = []
            for i in movies_recs:
                score = 0.0
                incre = 0
                decre = 0
                len = 0.0
                for j in user_rating:
                    a = getMovieRecs(uid, i[0], j[0], simMovies)
                    score += a * j[1]
                    if j[1] > 3.0:
                        incre += 1
                    else:
                        decre += 1
                    len += 1
                if decre == 0:
                    decre = 1
                    incre += 1
                    len += 2
                elif incre == 0:
                    incre = 1
                    decre += 1
                    len += 2
                ratings = (score / len) + math.log10(incre) - math.log10(decre)
                if ratings > 0.5:
                    recs_ls.append((i[0], ratings))
            return sorted(recs_ls, key=lambda x: x[1], reverse=True)

        def WriteToDB(rdd):
            """
            写入数据库
            :param rdd:
            :return:
            """
            if rdd.count() != 0:
                def Write(uid, ls):
                    new_ls = None
                    _con_db = pymysql.connect(
                        host=DB_HOST,
                        database=DB_DATABASE,
                        user=DB_USER,
                        password=DB_PASSWORD
                    )
                    _cursor = _con_db.cursor()
                    sql = "SELECT * FROM streamRecs WHERE uid={}".format(uid)
                    try:
                        c = _cursor.execute(sql)
                        if c != 0:
                            data = list(_cursor.fetchall())  # 获取原有数据
                            new_ls = data + ls
                            new_ls.sort(key=lambda x: x[1])
                            new_ls = list(dict(new_ls).items())
                            new_ls.sort(key=lambda x: x[1], reverse=True)
                            new_ls = new_ls[:15]

                        else:
                            new_ls = ls
                    except:
                        _con_db.rollback()
                        print("Error: unable to fetch data")

                    try:
                        sqld = "DELETE FROM streamRecs WHERE uid={}".format(uid)
                        _cursor.execute(sqld)
                        _con_db.commit()
                    except:
                        print("数据库操作失败，语句{}".format(sql1))
                        _con_db.rollback()


                    sql1 = "INSERT INTO streamRecs(uid, recs) VALUES ({}, '{}')".format(uid, str(new_ls))
                    try:
                        _cursor.execute(sql1)
                        _con_db.commit()
                        print("{}执行成功".format(sql1))
                        _con_db.close()
                    except:
                        print("数据库操作失败，语句{}".format(sql1))
                        _con_db.rollback()
                        _con_db.close()
                rdd.foreach(
                    lambda x: Write(x[0], x[1])
                )

        # 产生评分流
        data = self.receive()
        data.pprint()
        # write = self.streamWrite
        # user_recs_ratings = self.get
        data = data.map(
            lambda x: (int(x.split("|")[0]), int(x.split("|")[1]), eval(x.split("|")[2]), x.split("|")[3])
        )

        data.map(
            lambda x: (x[0], computeMovieScores(x[0], x[1], x[3], simMoviesMatrixBroadCast.value))
        ).foreachRDD(
            WriteToDB
        )

    def run(self):
        self.realHandle()
        self._ssc.start()
        self._ssc.awaitTermination()

    def test(self):
        def isRatingExist(uid, mid):
            """

            查询用户是否看过
            :param uid:
            :param mid:
            :return: bool
            """
            _con_db = pymysql.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            _cursor = _con_db.cursor()
            sql = "SELECT * FROM ratings WHERE uid={} AND mid={}".format(uid, mid)
            try:
                c = _cursor.execute(sql)
                _con_db.close()
                if c == 0:
                    return False
                else:
                    return True
            except:
                print("Error: unable to fetch data")
                _con_db.close()
                return None

        def getUserRecsRating(uid, t1, t2=30):
            """
            获取用户最近评分
            :param uid:
            :param t1:
            :param t2: 最近天数
            :return: rdd 最近评分
            """
            _con_db = pymysql.connect(
                host=DB_HOST,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            _cursor = _con_db.cursor()
            day = datetime.timedelta(days=-t2)
            date = datetime.datetime.fromtimestamp(eval(t1)) + day
            date = str(date).split(" ")[0] + " 00:00:00"
            sql = "SELECT * FROM ratings WHERE uid={} AND rating_time >= '{}'".format(uid, date)
            try:
                c = _cursor.execute(sql)
                if c == 0:
                    sql1 = "SELECT * FROM ratings WHERE uid={} limit 30".format(uid)
                    c = _cursor.execute(sql1)
                    if c == 0:
                        _con_db.close()
                        return None
                data = list(_cursor.fetchall())
                # print(data)
                _con_db.close()
                data = [(i[1], i[2]) for i in data]
                return data
            except:
                print("Error: unable to fetch data")
                _con_db.close()
                return None

        def getMovieRecs(uid, mid, smid, simMovies):
            """
            获取用户当前评分电影相似集合
            :param uid:
            :param mid:
            :param smid:
            :param simMovies: 相似矩阵
            :return:
            """
            # print(simMovies.keys())
            # if mid not in list(simMovies.keys()):
            #     return 0.0
            r = None
            try:
                r = [i for i in simMovies[mid] if i[0] == smid and isRatingExist(uid, i[0])]
            except:
                return 0.0
            if len(r) == 0:
                return 0.0
            else:
                return r[0][1]

        def computeMovieScores(uid, mid, t, simMovies):
            """
            计算电影评分
            :param uid:
            :param t:
            :param mid: 用户当前评分电影
            :param simMovies:
            :return:
            """
            user_rating = getUserRecsRating(uid, t)
            movies_recs = None
            try:
                movies_recs = simMovies[mid]  # 观看电影相似列表
            except:
                sql1 = "SELECT recs FROM userRecs WHERE uid={}".format(uid)
                _con_db = pymysql.connect(
                    host=DB_HOST,
                    database=DB_DATABASE,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                _cursor = _con_db.cursor()
                try:
                    c = _cursor.execute(sql1)
                    if c == 0:
                        _con_db.close()
                        return []
                    data = list(_cursor.fetchall())[0][0]
                    data = eval(data)
                    # print(data)
                    _con_db.close()
                    movies_recs = [(i[0], i[1]) for i in data]
                except:
                    print("Error: unable to fetch data")
                    _con_db.close()
            if user_rating is None:
                return movies_recs
            recs_ls = []
            for i in movies_recs:
                score = 0.0
                incre = 0
                decre = 0
                len = 0.0
                for j in user_rating:
                    a = getMovieRecs(uid, i[0], j[0], simMovies)
                    score += a * j[1]
                    if j[1] > 3.0:
                        incre += 1
                    else:
                        decre += 1
                    len += 1
                print(len, score, incre, decre)
                if decre == 0:
                    decre = 1
                elif incre == 0:
                    incre = 1
                ratings = (score / len) + math.log10(incre) - math.log10(decre)
                if ratings > 0.6:
                    recs_ls.append((i[0], ratings))
            return sorted(recs_ls, key=lambda x: x[1], reverse=True)

        movieResc = self.loadMovieRecs()
        simMoviesMatrixBroadCast = self._sc.broadcast(movieResc)
        computeMovieScores(20, 2151, "1643110257.9880092", simMoviesMatrixBroadCast.value)