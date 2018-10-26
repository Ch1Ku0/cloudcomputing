import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf
conf=SparkConf().setAppName("miniProject").setMaster("local[*]")
sc=sc.getOrCreate(conf)

rdd = sc.parallelize(["Hello hello", "Hello New York", "York says hello"])
resultRDD = (rdd
             .flatMap(lambda sentence:sentence.split(" "))
             .map(lambda word:word.lower())
             .map(lambda word: (word, 1))
             .reduceByKey(lambda x, y: x+y))
print(resultRDD.collect())
print(rdd.flatMap(lambda x: x.split(" ")).collect())

result = resultRDD.collectAsMap()
print(result)

print(resultRDD.sortByKey(ascending=True).take(3))

print(resultRDD
      .sortBy(lambda x: x[1], ascending=False)
      .take(2))