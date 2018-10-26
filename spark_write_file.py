from __future__ import print_function
from pyspark import SparkContext as sc
from pyspark import SparkConf
import sys
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("writeProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

def read_and_write():
    textFile = sc.textFile("file:///Users/chikuo/PycharmProjects/CloudComputing/googleplaystore.csv")
    # 取出每一列的列名 name是一个list
    name = textFile.first().split(",")
    print(name)
    # 写入hdfs
    i = 1
    while textFile.collect()[i] != None:
        temp_list = textFile.collect()[i].split(",")
        print(temp_list)
        temp_rdd = sc.parallelize(temp_list)
        temp_rdd.saveAsTextFile("hdfs://localhost:9000/user/test/"+ str(time.time()))
        i += 1






if __name__ == "__main__":
    read_and_write()


