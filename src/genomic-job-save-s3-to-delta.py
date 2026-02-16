import time
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

PARQUET_FILE = "s3a://genomic-shared/df_data.parquet"
BUCKET = "genomic"
DELTA_TABLE = "xxx-expression"
OUTPUT_PATH = f"s3a://{BUCKET}/{DELTA_TABLE}"

def sanitize(name: str) -> str:
    return re.sub(r"[^\w]", "_", name)

print("🟢 Configure Spark Session with AWS(Minio) support")
builder = (
    SparkSession.builder
        .appName("genomic-delta-ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]
).getOrCreate()

print("🟢 Configure Spark Session AWS(Minio) connection")
sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://spark-minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("🟢 Reading Parquet distributed dataset ans sanitize ...")
spark_df = spark.read.parquet(PARQUET_FILE)

sanitized_columns = [sanitize(c) for c in spark_df.columns]
spark_df = spark_df.toDF(*sanitized_columns)

print("🟢 Transforming wide → long format...")
id_cols = ["submitter_id", "cancer", "tumor"]
gene_cols = [c for c in spark_df.columns if c not in id_cols]

BATCH_SIZE = 300   # 🔥 tune this (100–500 depending on memory)
num_batches = (len(gene_cols) + BATCH_SIZE - 1) // BATCH_SIZE

print(f"Total gene columns: {len(gene_cols)}")
print(f"Processing in {num_batches} batches")

start_total = time.perf_counter()
for i in range(0, len(gene_cols), BATCH_SIZE):
    loop_start = time.perf_counter()

    batch_cols = gene_cols[i:i+BATCH_SIZE]

    print(f"🟢 Processing batch {i//BATCH_SIZE + 1}/{num_batches} with {len(batch_cols)} genes")
    # Cast to double only the needed columns
    df_batch = spark_df.select(
        *id_cols,
        *[col(c).cast("double").alias(c) for c in batch_cols]
    )

    # Build stack expression
    stack_expr = "stack({}, {}) as (gene, expression)".format(
        len(batch_cols),
        ",".join([f"'{c}', `{c}`" for c in batch_cols])
    )

    long_df = (
        df_batch.selectExpr(
            "submitter_id as sample_id",
            "cancer",
            "tumor",
            stack_expr
        )
        .where("expression IS NOT NULL AND expression != 0")
    )

    # Write each batch incrementally
    (
        long_df.write
            .format("delta")
            .mode("append")   # important: append each batch
            .save(OUTPUT_PATH)
    )

    # 🔥 free memory explicitly
    spark.catalog.clearCache()

    print(f"✅ Finished batch {i//BATCH_SIZE + 1}")

end_total = time.perf_counter()    
elapsed = end_total - start_total
print(f"✅ Total time {elapsed}")