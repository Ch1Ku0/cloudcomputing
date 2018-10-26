from __future__ import print_function
from pyspark import SparkContext as sc
from pyspark import SparkConf
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
conf = SparkConf().setAppName("miniProject").setMaster("local[*]")



# Spark Streaming可以通过Socket端口监听并接收数据，然后进行相应处理
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = sc.getOrCreate(conf)
    ssc = StreamingContext(sc, 3)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    # counts = lines.flatMap(lambda line: line.split(" "))\
    #               .map(lambda word: (word, 1))\
    #               .reduceByKey(lambda a, b: a+b)
    counts = lines #.flatMap(lambda line: line.split(" ")).map(lambda word: word.name)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()