from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

variables = sc.textFile("variables.txt")

k = int(variables.collect()[0])

file_path = variables.collect()[1]

path = variables.collect()[2]



text_file = sc.textFile("data.txt")

# worldCount = text_file.flatMap(lambda line: line.split(",")).map(lambda word: (word,1)).reduceByKey(lambda a,b : a+b)

worldCount = text_file.map(lambda line: (line.split(",")[0],float(line.split(",")[1]))).reduceByKey(lambda a,b : a+b)

result = worldCount.top(k, key=lambda x: x[1])

top_k = list(map(lambda x : x[0], result))

print(top_k)

# worldCount.repartition(1).saveAsTextFile("/Users/chikuo/first/nihao")
