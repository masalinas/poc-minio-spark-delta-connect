import time
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

start_time = time.process_time()

# Create Remote Spark Session from Spark Connect
builder = SparkSession.builder.appName("spark_connect_app") \
    .remote("sc://localhost:15002")

spark = builder.getOrCreate()

# Read Delta Table from Minio using spark connect
df = (
    spark.read.format("delta")
    .load("s3a://genomic/gene-expression")
    .where(col("sample_id") == "TCGA-E7-A7DV")
)

df.show()

# convert to Pandas
pdf = df.toPandas()
print("Dataset shape: " + str(pdf.shape))

print(pdf.head(5))

end_time= time.process_time()
print("Time spent: " + str(end_time - start_time) + " seconds")

# Stop spark
spark.stop()