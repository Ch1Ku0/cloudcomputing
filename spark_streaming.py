from pyspark import SparkContext as sc
from pyspark import SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setAppName("TestDStream").setMaster("local[*]")
sc = sc.getOrCreate(conf)
ssc = StreamingContext(sc, 10)

# lines = ssc.textFileStream('hdfs://localhost:9000/user/chikuo/')
lines = ssc.textFileStream('file:///Users/chikuo/PycharmProjects/CloudComputing/nihao/')

words = lines.flatMap(lambda line: line.split(' '))
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
