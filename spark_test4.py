import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

def doubleIfOdd(x):
    if x % 2 == 1:
        return 2 * x
    else:
        return x

numbersRDD = sc.parallelize(range(1,10+1))
resultRDD = (numbersRDD.map(doubleIfOdd).filter(lambda x : x >6).distinct())

print(resultRDD.glom().collect())