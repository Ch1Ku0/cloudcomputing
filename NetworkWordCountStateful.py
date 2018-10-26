from __future__ import print_function

import sys
import pymongo

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: stateful_network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingStatefulNetworkWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("file:///Users/chikuo/PycharmProjects/CloudComputing/nihao/")

    # RDD with initial state (key, value) pairs
    initialStateRDD = sc.parallelize([(u'hello', 1), (u'world', 1)])


    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)


    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    running_counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .updateStateByKey(updateFunc, initialRDD=initialStateRDD)

    # running_counts.pprint()
    print(running_counts)


    def dbfunc(records):
        # 1.连接数据库服务器,获取客户端对象
        mongo_client = pymongo.MongoClient('localhost', 27017)
        # 2.获取数据库对象
        db = mongo_client.runoob
        # 3.获取集合对象
        my_collection = db.col

        def doinsert(p):
            temp = {p[0]: p[1]}
            print(p[0]+"------"+str(p[1]))
            item_id = my_collection.insert(temp)

        for item in records:
            doinsert(item)


    def func(rdd):
        repartitionedRDD = rdd.repartition(3)
        repartitionedRDD.foreachPartition(dbfunc)


    running_counts.foreachRDD(func)
    ssc.start()
    ssc.awaitTermination()
