import pymongo
from pyspark import SparkContext as sc
from pyspark import SparkConf

conf = SparkConf().setAppName("pymongo_Project").setMaster("local[*]")
sc = sc.getOrCreate(conf)
# 1.连接数据库服务器,获取客户端对象
mongo_client = pymongo.MongoClient('localhost', 27017)

# 2.获取数据库对象
db = mongo_client.runoob

# 3.获取集合对象
my_collection = db.col

# 插入文档
# tom = {'name': 'Tom', 'age': 18, 'sex': '男', 'hobbies': ['吃饭', '睡觉', '打豆豆']}
# tom_id = my_collection.insert(tom)

cursor = my_collection.find()

print(my_collection.find_one().get("_id"))
for item in cursor:
    print(item)

data = sc.parallelize(my_collection.find_one().get("hobbies"))

print(data.collect())







