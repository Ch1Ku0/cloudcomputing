from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sc = SparkContext(appName="PythonStreaming")
ctx = SQLContext(sc)
test_collection = ctx.read.format("com.mongodb.spark.sql").options(uri="mongodb://localhost:27017", database="runoob", collection="col").load()
