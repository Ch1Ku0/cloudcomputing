import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

numberRDD = sc.parallelize([1,2,3])
moreNumbersRDD = sc.parallelize([2,3,4])

print(numberRDD.union(moreNumbersRDD).collect())
print(numberRDD.intersection(moreNumbersRDD).collect())
print(numberRDD.subtract(moreNumbersRDD).collect())
print(numberRDD.cartesian(moreNumbersRDD).collect())
