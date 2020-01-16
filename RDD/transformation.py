from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[2]").setAppName("map")

sc = SparkContext(conf=conf)


def map1():
    data = [1, 2, 3, 4, 5]
    rdd1 = sc.parallelize(data)
    rdd2 = rdd1.map(lambda x : x * 2)
    print(rdd2.collect())


def map2():
    data = ["dog", "cat", "tiger", "rabbit", "python"]
    rdd1 = sc.parallelize(data)
    rdd2 = rdd1.map(lambda x: (x, 1))
    print(rdd2.collect())


def filter1():
    data = [1, 2, 3, 4, 5]
    rdd1 = sc.parallelize(data)
    rdd2 = rdd1.filter(lambda x: x > 3)
    print(rdd2.collect())


def flatmap1():
    data = ["Hello Spark", "Hello World", "Hello World"]
    rdd = sc.parallelize(data)
    mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
    groupByRdd = mapRdd.groupByKey()
    print(groupByRdd.map(lambda x: {x[0]: list(x[1])}).collect())


def reducebykey():
    data = ["Hello Spark", "Hello World", "Hello World"]
    rdd = sc.parallelize(data)
    mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
    reducerdd = mapRdd.reduceByKey(lambda a, b: a + b)
    print(reducerdd.collect())


def sortbykey():
    data = ["Hello Spark", "Hello World", "Hello World"]
    rdd = sc.parallelize(data)
    mapRdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
    reducerdd = mapRdd.reduceByKey(lambda a, b: a + b)

    print(reducerdd.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0])).collect())


def myunion():
    a = sc.parallelize([1, 2, 3])
    b = sc.parallelize([3, 4, 5])
    print(a.union(b).collect())
    print(a.union(b).distinct().collect())


def my_join():
    a = sc.parallelize([("A", "a1"), ("C", "c1"), ("D", "d1"), ("F", "f1"), ("F", "f2")])
    b = sc.parallelize([("A", "a2"), ("C", "c2"), ("C", "c3"), ("E", "e1")])
    # print(a.join(b).collect())
    # print(a.leftOuterJoin(b).collect())
    # print(a.rightOuterJoin(b).collect())
    print(a.fullOuterJoin(b).collect())


my_join()
sc.stop()
