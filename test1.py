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

def h():
    a = Handle()
    while True:
        a.run()
        time.sleep(60 * 60 * 24 * 15)

def real(sc):
    a = RealTimeHandle(sc)
    a.run()

a = Handle()
a.run()
# a = RealTimeHandle()
# a.run()

# if __name__ == "__main__":
#     _conf = SparkConf().setAppName("stream").setMaster("yarn")
#     _sc = SparkContext(conf=_conf)
#     _spark = SparkSession(_sc)
#     t1 = threading.Thread(target=h, args=(_spark,))
#     t2 = threading.Thread(target=real, args=(_sc,))
#     t1.setDaemon(True)
#     t2.setDaemon(True)
#     t1.start()
#     t2.start()
#     while True:
#         pass