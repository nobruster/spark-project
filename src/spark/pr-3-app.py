from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

df_users = spark.read.json("users.json")
count = df_users.count()
df_users.show(3)

spark.stop()