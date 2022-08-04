# encodinng = utf-8

"""
    @describe: 实时处理
"""

from pyspark import (SparkConf, SparkContext)
from pyspark.streaming import (StreamingContext, DStream)
from pyspark.sql import (SparkSession)
import os
import math
import datetime
import time
import threading
from packages.conf.realtimeconf import *
from packages.Handle.Handle import *
from packages.Handle.RealTimeHandle import *

def h(spark):
    a = Handle(spark=spark)
    while True:
        a.run()
        time.sleep(60 * 60 * 24 * 15)

def real(sc):
    a = RealTimeHandle(sc)
    a.run()

a = Handle()
a.test()
