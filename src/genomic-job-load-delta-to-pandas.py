import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

start_time = time.perf_counter()

print("🟢 Configure Spark Session with AWS(Minio) support")
builder = SparkSession.builder.appName("spark_job_load_genomic_app") \
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

print("🟢 Read Delta Table from Minio using spark connect")
row_count = (
    spark.read.format("delta")
    .load("s3a://genomic/xxx-expression")
    .count()
)

print("🟢 Show row count")
print(row_count)

df = (
    spark.read.format("delta")
    .load("s3a://genomic/gene-expression")
    .where(col("sample_id") == "TCGA-E7-A7DV")
)

print("🟢 Show Spark Dataframe filterred")
df.show()

end_time= time.perf_counter()
print("Time spent: " + str(end_time - start_time) + " seconds")

# convert to Pandas
start_time = time.perf_counter()

print("🟢 Convert Spark Dataframe to Pandas")
pdf = df.toPandas()
print("Dataset shape: " + str(pdf.shape))

print(pdf.head(5))

end_time= time.perf_counter()
print("Time spent: " + str(end_time - start_time) + " seconds")

# Stop spark
spark.stop()