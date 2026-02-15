import time
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

print("🟢 Configure Spark Session with AWS(Minio) support")
builder = SparkSession.builder.appName("spark_job_save_mock") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]) \
    .getOrCreate()
 
print("🟢 Configure Spark Session AWS(Minio) connection")
sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://spark-minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Create a DataFrame
print("🟢 Create mock Dataframe")
df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

start_time = time.perf_counter()

# Write Delta Table to Minio using Spark Connect
print("🟢 Write Dataframe to Delta tables")
df.write.mode("overwrite").format("delta").save("s3a://delta-bucket/my_table")

# Read Delta Table from Minio
print("🟢 Read Dataframe to Delta tables")
df = spark.read.format("delta").load("s3a://delta-bucket/my_table")

df.show()

end_time= time.perf_counter()
print(end_time - start_time, " seconds")

# Stop spark
spark.stop()