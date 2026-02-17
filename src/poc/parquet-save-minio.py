import time
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

INPUT_PATH = "s3a://genomic-shared/df_data.parquet"
OUTPUT_PATH = "s3a://genomic-shared/df_data_long.parquet"

def sanitize(name: str) -> str:
    return re.sub(r"[^\w]", "_", name)

start_time= time.perf_counter()

print("🟢 Create local Spark Session")
builder = SparkSession.builder \
    .appName("save-parquet-minio")

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

print("🟢 Identify columns")
spark_df = spark_df.drop('cancer#')
sanitized_columns = [sanitize(c) for c in spark_df.columns]
spark_df = spark_df.toDF(*sanitized_columns)

meta_cols = ["submitter_id", "cancer",  "tumor"]
all_cols = spark_df.columns

gene_cols = [c for c in all_cols if c not in meta_cols]

print(f"🟢 Gene columns: {len(gene_cols)}")

print("🟢 Cast to double only the needed columns")
spark_df = spark_df.select(
    *meta_cols,
    *[col(c).cast("double").alias(c) for c in gene_cols]
)

print("🟢 Build STACK expression")
stack_expr = "stack({0}, {1}) as (gene, expression)".format(
    len(gene_cols),
    ",".join([f"'{c}', `{c}`" for c in gene_cols])
)

print("🟢 Transforming to long format")
df_long = spark_df.selectExpr(
    "submitter_id as sample_id",
    "cancer",
    "tumor",
    stack_expr
)

print("🟢 Clean data")
df_long = df_long.filter("expression is not null AND expression != 0")
df_long = df_long.withColumn("expression", df_long["expression"].cast("float"))

print("🟢 Write as PARQUET (no delta)")
(
    df_long
        #.repartition(12)   # tune for your cluster
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
)

print("✅ Conversion finished")

end_time= time.perf_counter()
print(end_time - start_time, " seconds")

spark.stop()