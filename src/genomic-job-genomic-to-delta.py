import time
import math
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from delta import configure_spark_with_delta_pip

H5_FILE = "/jobs/datasets/df_data.hdf"
BUCKET = "genomic"
DELTA_TABLE = "test-expression"

# Configure Spark Session with AWS(Minio) support
builder = SparkSession.builder.appName("minio_job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]).getOrCreate()

# Configure Spark Session AWS(Minio) connection
sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://spark-minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("🟢 Read H5 Dataset")
pdf = pd.read_hdf(H5_FILE)

print("🟢 Define Delta Table Scheme")  
schema = StructType([
    StructField("sample_id", StringType(), True),
    StructField("cancer", StringType(), True),
    StructField("tumor", StringType(), True),
    StructField("gene", StringType(), True),
    StructField("expression", FloatType(), True)
])

print("🟢 Start save chunks dataset loop")  
path = "s3a://" + BUCKET + "/" + DELTA_TABLE

CHUNK_SIZE = 100
total_rows = pdf.shape[0]
num_chunks = math.ceil(total_rows / CHUNK_SIZE)
start_total = time.perf_counter()

print("Num chunks: " + str(num_chunks))

for i in range(num_chunks):
    loop_start = time.perf_counter()

    # Slice wide Pandas
    start = i * CHUNK_SIZE
    end = min((i + 1) * CHUNK_SIZE, total_rows)

    pdf_chunk = pdf.iloc[start:end]

    # Wide → long (Pandas)
    pdf_chunk = pdf_chunk.reset_index(level=0)
    pdf_chunk = pdf_chunk.drop("cancer#", axis=1)
    pdf_chunk = pdf_chunk.set_index(["submitter_id", "cancer", "tumor"])

    pdf_long = (
        pdf_chunk
            .stack()
            .reset_index()
    )

    pdf_long.columns = ["sample_id", "cancer", "tumor", "gene", "expression"]

    # Optional: drop zeros / NaNs and set float type for expression column
    pdf_long = pdf_long[pdf_long["expression"].notna()]
    pdf_long = pdf_long[pdf_long["expression"] != 0]
    pdf_long["expression"] = pdf_long["expression"].astype(float)

    # Pandas → Spark
    print("🟢 Create Spark Chunk Dataframe from Pandas")
    spark_df = spark.createDataFrame(pdf_long, schema=schema)

    # Append to Delta
    print("🟢 Append Chunk Dataframe to Delta")
    (
        spark_df
            .repartition(4)
            .write
            .mode("append")            
            .format("delta")
            .save(path)
    )

    loop_end = time.perf_counter()

    # Progress log
    loop_time = loop_end - loop_start
    elapsed = loop_end - start_total
    percent = (end / total_rows) * 100

    print(
        f"Chunk {i+1}/{num_chunks} | "
        f"Samples {start}-{end} | "
        f"{percent:.2f}% | "
        f"Chunk time: {loop_time:.2f}s | "
        f"Elapsed: {elapsed/60:.2f} min"
    )