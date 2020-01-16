from pyspark import SparkContext, SparkConf


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("map")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("C:/Users/Administrator/PycharmProjects/Spark/data/sample_age_data.txt")
    ageData = rdd.map(lambda line: line.split(" ")[1])
    sumAge = ageData.map(lambda x: int(x)).sum()
    counts = ageData.count()
    averageAge = sumAge / counts

    print(counts)
    print(sumAge)
    print(averageAge)

    sc.stop()
