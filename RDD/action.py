from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[2]").setAppName("map")

sc = SparkContext(conf=conf)


def demo():
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    rdd = sc.parallelize(data)
    # print(rdd.collect())
    # print(rdd.count())
    # print(rdd.take(3))
    # print(rdd.max())
    # print(rdd.min())
    # print(rdd.sum())
    rdd.foreach(lambda x: print(x))


demo()
sc.stop()

