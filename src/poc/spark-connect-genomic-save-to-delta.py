import time
import math
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

H5_FILE = "/Users/miguel/Temp/genomic/df_data.hdf"
BUCKET = "genomic"
DELTA_TABLE = "test-expression"

spark = SparkSession.builder.appName("spark_connect_gen") \
    .remote("sc://localhost:15002") \
    .getOrCreate()

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

    #data = {
    #    "calories": [420, 380, 390],
    #    "duration": [50, 40, 45]
    #}

    #load data into a DataFrame object:
    #df = pd.DataFrame(data)

    # Pandas → Spark
    print("🟢 Create Spark Chunk Dataframe from Pandas")
    spark_df = spark.createDataFrame(pdf_long, schema=schema)
    #spark_df1 = spark.createDataFrame(df)

    print("🟢 Append Chunk Dataframe to Delta")
    (
        spark_df
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

spark.stop()