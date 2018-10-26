from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

lines = sc.textFile("word.txt")
pairRDD = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

# pairRDD.reduceByKey(lambda a,b : a+b).foreach(print)
#
# pairRDD.groupByKey().foreach(print)
#
# pairRDD.keys().foreach(print)
#
# pairRDD.values().foreach(print)

rdd = sc.parallelize([("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)])
print(rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(
    lambda x: (x[0] / x[1])).collect())
