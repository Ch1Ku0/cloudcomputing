from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os

# os.environ['PYSPARK_PYTHON'] = '/Users/chenhao/anaconda3/bin/python'


def connectToMongo(master, inputuri, outputuri):
    sparkMongo = SparkSession.builder.master(master).appName("myApp") \
        .config("spark.mongodb.input.uri", inputuri) \
        .config("spark.mongodb.output.uri", outputuri) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
        .getOrCreate()

    # writeData = sparkMongo.createDataFrame([('zhangwei', 24), ('zhangjiajie', 23)], ['name', 'age'])
    # writeData.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
    # writeData.show()
    data = sparkMongo.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://localhost:27017/runoob.col").load()
    data.show()


if __name__ == '__main__':
    inputuri = "mongodb://localhost:27017/runoob.runoob"
    outputuri = "mongodb://localhost:27017/runoob.runoob"
    master = "local[*]"
    connectToMongo(master, inputuri, outputuri)
