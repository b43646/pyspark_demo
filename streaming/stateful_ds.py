# Import the necessary classes and create a local SparkContext and Streaming Contexts
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create Spark Context with two working threads (note, `local[2]`)
sc = SparkContext("local[2]", "StatefulNetworkWordCount")

# Create local StreamingContextwith batch interval of 1 second
ssc = StreamingContext(sc, 1)

# Create checkpoint for local StreamingContext
ssc.checkpoint("checkpoint")


# Define updateFunc: sum of the (key, value) pairs
def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


# Create DStream that will connect to the stream of input lines from connection to localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

running_counts = lines.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (word, 1))\
    .updateStateByKey(updateFunc)

# Print the first ten elements of each RDD generated in this stateful DStream to the console
running_counts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()