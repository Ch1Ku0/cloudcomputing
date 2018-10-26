from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

text_file = sc.textFile("hdfs://localhost:9000/user/chikuo/evaluate.txt")
print(text_file.first())

worldCount = text_file.flatMap(lambda line: line.split("\n")).map(lambda word: (word,1)).reduceByKey(lambda a,b : a+b)

print(worldCount.glom().collect())