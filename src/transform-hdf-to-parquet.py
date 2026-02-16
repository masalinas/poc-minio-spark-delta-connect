import pandas as pd

H5_FILE = "/Users/miguel/git/poc-spark-cluster-minio/datasets/df_data.hdf"
PARQUET_FILE= "/Users/miguel/git/poc-spark-cluster-minio/datasets/df_data.parquet"

print("Reading HDF5...")
df = pd.read_hdf(H5_FILE)

print("Writing Parquet...")
df.to_parquet(PARQUET_FILE)

print("Done")