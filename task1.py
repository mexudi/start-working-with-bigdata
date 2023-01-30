from pyspark.sql import SparkSession

spark = SparkSession \
        .buidler \
        .master("local[1]") \
        .appName("myApp") \
        .getOrCreate()

df = spark.read.csv("./data.csv", header=True)
df.show()
