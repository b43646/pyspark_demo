from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("map")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("C:/Users/Administrator/PycharmProjects/Spark/data/page_views.dat")
    counts = rdd.map(lambda line: line.split("\t"))\
        .map(lambda x: (x[5], 1))\
        .reduceByKey(lambda a, b: a + b)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(False)\
        .map(lambda x: (x[1], x[0]))\
        .take(5)

    for i in counts:
        print(i)
    # rdd2.saveAsTextFile("file:///C:/Users/Administrator/PycharmProjects/Spark/result")

    sc.stop()
