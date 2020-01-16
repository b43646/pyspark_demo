from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext("local[2]", "wordcount")
ssc = StreamingContext(sc, 5)
lines = ssc.textFileStream("file:///root/data2/")
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordcounts = pairs.reduceByKey(lambda x, y: x + y)

wordcounts.saveAsTextFiles("/root/data2/")
wordcounts.pprint()

ssc.start()
ssc.awaitTermination()

