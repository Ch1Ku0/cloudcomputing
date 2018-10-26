import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf
conf=SparkConf().setAppName("miniProject").setMaster("local[*]")
sc=sc.getOrCreate(conf)

#（a）记录当前pyspark工作环境位置
rdd=sc.textFile("variables.txt")
print(rdd)

print(rdd.collect()[1])
