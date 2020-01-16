from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("map")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("C:/Users/Administrator/PycharmProjects/Spark/data")
    rdd2 = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    print(rdd2.collect())
    # rdd2.saveAsTextFile("file:///C:/Users/Administrator/PycharmProjects/Spark/result")

    sc.stop()

