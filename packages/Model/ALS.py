# encoding = utf-8

"""
    @describe: ALS模型
"""

from packages.Model.Model import *

from pyspark.sql import (SparkSession)
from pyspark.mllib.recommendation import ALS

import numpy as np
import pandas as pd

class Als(Model):

    _model = None

    def __init__(self):
        super(Model, self).__init__()

    def buildModel(self, data_rdd, rank=200, iterations=5, lambda_=0.01, blocks=- 1, seed=None):
        """
        构建模型
        :param data_rdd:
        :param rank:要使用的特征数量
        :param iterations:迭代次数
        :param lambda_:正则化参数
        :param blocks:正则化参数
        :param seed:初始矩阵分解模型的随机种子
        :return:
        """
        self._model = ALS.train(data_rdd,
                  rank=rank,
                  iterations=iterations,
                  lambda_=lambda_,
                  blocks=blocks,
                  seed=seed
                  )

    def train(self, data_rdd, rank=80, iterations=5, lambda_=0.01, blocks=- 1, seed=None):
        """
        训练模型
        :param data_rdd:
        :param rank:要使用的特征数量
        :param iterations:迭代次数
        :param lambda_:正则化参数
        :param blocks:正则化参数
        :param seed:初始矩阵分解模型的随机种子
        :return:
        """
        self.buildModel( data_rdd, rank, iterations, lambda_, blocks, seed)

    def predict(self, data_rdd):
        """
        分解后矩阵的值
        :param data_rdd: 空用户电影矩阵
        :return:
        """
        return self._model.predictAll(data_rdd)

    def save(self, sc, path):
        """
        保存模型
        :param sc:
        :param path:保存路径
        :return:
        """
        self._model.save(sc, path)

    def get_model(self):
        return self._model

    def adjustModel(self, test_data):
        """
        评估模型
        :param test_data:
        :return:
        """
        data = test_data.map(
            lambda x: (x[0], x[1])
        )
        pre = self.predict(data)

        observed = test_data.map(lambda x: ((x[0], x[1]), x[2]))
        predict_rating = pre.map(
            lambda item: ((item.user, item.product), item.rating)
        )
        a = observed.join(predict_rating).map(
            lambda x: ((float(x[1][0]) - float(x[1][1])) ** 2)
        ).collect()
        return sum(a) / len(a)

    def test(self):
        """
        测试模型
        :param sql:
        :return:
        """
        pass