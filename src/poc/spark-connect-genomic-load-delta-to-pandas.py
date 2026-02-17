import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BUCKET = "genomic"
DELTA_TABLE = "test-expression"

start_time = time.process_time()

print("🟢 Create Remote Spark Session from Spark Connect")
spark = SparkSession.builder.appName("spark_connect_app") \
    .remote("sc://localhost:15002") \
    .getOrCreate()

print("🟢 Read Delta Table from Minio using spark connect")  
path = "s3a://" + BUCKET + "/" + DELTA_TABLE

df = (
    spark.read.format("delta")
    .load(path)
    .where(col("sample_id") == "TCGA-E7-A7DV")
)

df.show()

end_time= time.process_time()
print("Time spent: " + str(end_time - start_time) + " seconds")

start_time = time.process_time()

print("🟢 convert to Pandas and show")
pdf = df.toPandas()

print("Dataset shape: " + str(pdf.shape))
print(pdf.head(5))

end_time= time.process_time()
print("Time spent: " + str(end_time - start_time) + " seconds")

spark.stop()