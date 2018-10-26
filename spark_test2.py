import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

numbersRDD = sc.parallelize(range(1, 10 + 1))
print(numbersRDD.collect())

#map()对RDD的每一个item都执行同一个操作
squareRDD = numbersRDD.map(lambda x: x**2)
print(squareRDD.collect())

#filter()筛选出来满足条件的item
filteredRDD = numbersRDD.filter(lambda x: x % 2 == 0)
print(filteredRDD.collect())
