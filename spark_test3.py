import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

# flatMap() 对RDD中的item执行同一个操作以后得到一个list
# 然后以平铺的方式把这些list里所有的结果组成新的list

sentencesRDD = sc.parallelize(['Hello world','My name is nidie'])
wordsRDD = sentencesRDD.flatMap(lambda sentence: sentence.split(" "))
print(wordsRDD.collect())
print(wordsRDD.count())
