from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Spark-Test.Numbers") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Spark-Test.Numbers") \
    .getOrCreate()
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

