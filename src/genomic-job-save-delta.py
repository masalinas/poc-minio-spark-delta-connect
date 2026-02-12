import time
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

print("🟢 Create spark Session from job container")
spark = SparkSession.builder.appName("spark_connect_app") \
    .remote("sc://spark-master:15002").getOrCreate()

# Create a DataFrame
print("🟢 Create mock Dataframe")
df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

start_time = time.process_time()

# Write Delta Table to Minio using Spark Connect
print("🟢 Write Dataframe to Delta tables")
df.write.mode("overwrite").format("delta").save("s3a://delta-bucket/my_table")

# Read Delta Table from Minio
print("🟢 Read Dataframe to Delta tables")
df = spark.read.format("delta").load("s3a://delta-bucket/my_table")

df.show()

end_time= time.process_time()
print(end_time - start_time, " seconds")

# Stop spark
spark.stop()