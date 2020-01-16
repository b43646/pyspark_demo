from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("startup")
# conf = SparkConf().setMaster("spark://hadoop001:7077").setAppName("startup")

sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print(distData.collect())
# print(distData.map(lambda a: len(a)).reduce(lambda a, b: a + b))

# distFile = sc.textFile("hello.txt")
#
# print(distFile.map(lambda a: len(a)).reduce(lambda a, b: a + b))

sc.stop()


