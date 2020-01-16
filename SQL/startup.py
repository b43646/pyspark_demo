from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

    df = spark.read.json("C:/Users/Administrator/PycharmProjects/Spark/SQL/data/people.json")
    # df.show()
    # df.printSchema()
    # df.select("name").show()
    # df.select(df["name"], df['age'] + 1).show()
    # df.filter(df['age'] > 21).show()
    # df.groupBy('age').count().show()

    df.createOrReplaceTempView("people")
    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

