import time
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

builder = SparkSession.builder.appName("spark_connect_app") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .remote("sc://localhost:15002")

spark = builder.getOrCreate()

# Create a DataFrame
df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

# Write Delta Table to Minio
#df.write.mode("overwrite").format("delta").save("s3a://delta-bucket/my_table")

# Read Delta Table from Minio
start_time = time.process_time()
#df = spark.read.format("delta").load("s3a://delta-bucket/my_table")

df = (
    spark.read.format("delta")
    .load("s3a://genomic/gene-expression")
    .where(col("sample_id") == "TCGA-E7-A7DV")
)

# Display Delta Table
df.show()

pdf = df.toPandas()
print(pdf.shape)
end_time= time.process_time()

print(end_time - start_time, " seconds")

spark.stop()