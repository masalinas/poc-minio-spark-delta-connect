import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

INPUT_PATH = "s3a://genomic-shared/df_data_long.parquet"

start_time= time.perf_counter()

builder = SparkSession.builder \
    .appName("load-parquet-minio")

spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]) \
    .getOrCreate()

sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark_df = spark.read.parquet(INPUT_PATH)

# ----------------------------------
# Read PARQUET (no delta)
# ----------------------------------
spark_df.createOrReplaceTempView("genomic")

result = spark.sql("""
    SELECT *
    FROM genomic
    WHERE sample_id = 'TCGA-E7-A7DV'
""")

result.show()

end_time= time.perf_counter()
print(end_time - start_time, " seconds")
