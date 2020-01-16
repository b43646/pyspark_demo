from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("data/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
schema = StructType(fields)

schemaPeople = spark.createDataFrame(people, schema)
schemaPeople.createOrReplaceTempView("people")
results = spark.sql("SELECT name FROM people")
results.show()
