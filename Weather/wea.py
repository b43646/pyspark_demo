from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def get_grade(value):
    if value <= 50 and value >= 0:
        return "健康"
    elif value <= 100:
        return "中等"
    elif value <= 150:
        return "对敏感人群不健康"
    elif value <= 200:
        return "不健康"
    elif value <= 300:
        return "非常不健康"
    elif value <= 500:
        return "危险"
    elif value > 500:
        return "爆表"
    else:
        return None


if __name__ == '__main__':
    spark = SparkSession.builder.appName("weather").getOrCreate()

    data2017 = spark.read.format("csv").option("header","true").option("inferSchema","true")\
        .load("file:///C:/Users/Administrator/PycharmProjects/Spark/Weather/data/Beijing_2017_HourlyPM25_created20170803.csv")\
        .select("Year","Month","Day","Hour","Value","QC Name")
    data2016 = spark.read.format("csv").option("header","true").option("inferSchema","true")\
        .load("file:///C:/Users/Administrator/PycharmProjects/Spark/Weather/data/Beijing_2016_HourlyPM25_created20170201.csv")\
        .select("Year","Month","Day","Hour","Value","QC Name")
    data2015 = spark.read.format("csv").option("header","true").option("inferSchema","true")\
        .load("file:///C:/Users/Administrator/PycharmProjects/Spark/Weather/data/Beijing_2015_HourlyPM25_created20160201.csv")\
        .select("Year","Month","Day","Hour","Value","QC Name")

    # data2017.show()
    # data2016.show()
    # data2015.show()

    grade_function_udf = udf(get_grade, StringType())

    group2017 = data2017.withColumn("grade", grade_function_udf(data2017['Value'])).groupBy("grade").count()
    group2016 = data2016.withColumn("grade", grade_function_udf(data2016['Value'])).groupBy("grade").count()
    group2015 = data2015.withColumn("grade", grade_function_udf(data2015['Value'])).groupBy("grade").count()
    # group2017.show()
    # group2016.show()
    # group2015.show()
    res2017 = group2017.select("grade", "count").withColumn("percent", group2017['count'] / data2015.count() * 100)
    res2016 = group2016.select("grade", "count").withColumn("percent", group2016['count'] / data2015.count() * 100)
    res2015 = group2015.select("grade", "count").withColumn("percent", group2015['count'] / data2015.count() * 100)

    # res2015.show()
    # res2016.show()
    # res2017.selectExpr("grade as abc", 'count', 'percent').show()
    # res2015.write.format("org.elasticsearch.spark.sql")\
    #     .option("es.nodes", "192.168.1.206:9200").mode("overwrite").save("weather2015/pm")

    for i in res2015.collect():
        print(i['grade'])
    # res2015.show()
