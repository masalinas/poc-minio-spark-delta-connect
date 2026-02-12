import random
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("spark_connect_pi").remote("sc://localhost:15002")
spark = builder.getOrCreate()

n = 200000
df = spark.createDataFrame([(random.random(), random.random()) for _ in range(n)], ["x", "y"])

# Compute pi using DataFrame API
count = df.filter("x*x + y*y < 1").count()
print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()