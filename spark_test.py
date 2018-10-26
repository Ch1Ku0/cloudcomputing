import pyspark
from pyspark import SparkContext as sc
from pyspark import SparkConf

# 任何Spark程序都是SparkContext开始的，SparkContext的初始化需要一个SparkConf对象
# SparkConf包含了Spark集群配置的各种参数(比如主节点的URL)。
# 初始化后，就可以使用SparkContext对象所包含的各种方法来创建和操作RDD和共享变量。
# Spark shell会自动初始化一个SparkContext(在Scala和Python下可以，但不支持Java)。
# getOrCreate表明可以视情况新建session或利用已有的session
# spark://localhost:7077
conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = sc.getOrCreate(conf)

def computing():
    # （a）利用list创建一个RDD;
    # 使用sc.parallelize可以把Python list，NumPy array或者Pandas Series,Pandas DataFrame转成Spark RDD。
    data = [1, 2, 3, 4, 5, 4, 2, 1, 3, 2, 6, 8, 4, 2]
    rdd = sc.parallelize(data)
    rdd.distinct()

    print(rdd.collect())

if __name__ == '__main__':
    computing()

