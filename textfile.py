from pyspark import SparkContext
import json

sc = SparkContext('local','JSONAPP')
inputFile = "/usr/local/Cellar/spark/examples/src/main/resources/people.json"
jsonStr = sc.textFile(inputFile)
jsonStr.foreach(print)