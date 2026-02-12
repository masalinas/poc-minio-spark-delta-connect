# Description
Integrate Minio with Spark Delta Lake and Spark Connect. Test with a Spark Driver

## Python Spark Master and Worker dependencies
We will use the Spark 3.5.0 using the [Spark Docker Image](https://hub.docker.com/_/spark): `spark:3.5.0-python3`

## Python Spark Client dependencies
We must install pyspark 3.5.0 aligned with Spark Docker Image 3.5.0 including these dependencies:

```
pip install pyspark==3.5.0
pip install pandas
pip install pyarrow
pip install googleapis-common-protos
pip install grpcio 
pip install grpcio-tools
pip install grpcio-status
pip install protobuf
```

The dependencies Google gRPC because the protocol used by the Sparck Clients and the Spark Master is using gRPC 
```
googleapis-common-protos
grpcio
grpcio-tools
grpcio-status
protobuf
```

The dependency `pyarrow` because the response from Spark Master is using gRPC throw pyarrow to accelerate this response

And finally `pandas` to convert Spark Dataframe response to Pandas structures

## Steps

You must start these services

- Start Minio Service. And create a bucket called in my case `genomic`. Spark Delta Lake create the table, but not the bucket, you must create manually from Minio UI or from Python (In this case you can use mc minio CLI or from Python using minio SDK).

- Create docker network
  ```
  $ docker network create spark-net
  ```

- Build Spark Master image. We must create a custom Spark Image, because we must include Spark Connector, Spark Deta Lake and Minio Hadoop Connector dependencies and configure Spark Session for all of them:

  ```
  $ docker build -t spark:3.5.0-python3-connect .
  ```

  ```
  $ docker build -t spark:3.5.0-python3-supply .
  ```

- Start Spark Master Service
    
In docker 20.10.5 (Ubuntu 20.04), we must use a older minio version to start:
  ```
  $ docker run -d \
    --name spark-minio \
    --network spark-net \
    -p 9000:9000 \
    -p 9001:9001 \
    -e MINIO_ROOT_USER=admin \
    -e MINIO_ROOT_PASSWORD=password \
    -v minio-data:/data \
    minio/minio:RELEASE.2025-01-20T14-49-07Z \
      server --console-address ":9001" /data 
  ```

In docker 28.1.1 (Ubuntu 24.04)
  ```
  $ docker run -d \
    --name spark-minio \
    --network spark-net \
    -p 9000:9000 \
    -p 9001:9001 \
    -e MINIO_ROOT_USER=admin \
    -e MINIO_ROOT_PASSWORD=password \
    -v minio-data:/data \
    minio/minio:latest \
      server --console-address ":9001" /data 
  ```

- Start Spark Master Service
  ```
  $ docker run -d \
    --name spark-master \
    --network spark-net \
    -p 7077:7077 \
    -p 8080:8080 \
    -p 15002:15002 \
    -v $PWD/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
    spark:3.5.0-python3-connect \
      bash -c "
        /opt/spark/sbin/start-master.sh && \
        /opt/spark/sbin/start-connect-server.sh && \
        tail -f /opt/spark/logs/*
      "
```

- Start Spark Worker Service
  ```
  $ docker run -d \
      --name spark-worker \
      -p 8081:8081 \
      -v $PWD/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
      --network spark-net \
      spark:3.5.0-python3-connect \
        bash -c "
          /opt/spark/sbin/start-worker.sh \
          spark://spark-master:7077 && \
          tail -f /opt/spark/logs/*
        "
  ```

- Submit Spark Driver App
  ```
  $ docker run -it \
      --name spark-genomic-job \
      --rm \
      -v $PWD/src/genomic-job-genomic-to-delta.py:/jobs/genomic-job-genomic-to-delta.py \
      -v $PWD/datasets/df_data.hdf:/jobs/datasets/df_data.hdf \
      --network spark-net \
      spark:3.5.0-python3-supply \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/genomic-job-genomic-to-delta.py
  ```

  ```
  $ docker run -it \
      --name spark-mock-job \
      --rm \
      -v $PWD/src/genomic-job-mock-to-delta.py:/jobs/genomic-job-mock-to-delta.py \
      --network spark-net \
      spark:3.5.0-python3-supply \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/genomic-job-mock-to-delta.py
  ```      

You can check the UIs of Minio and Spark:

![Spark Docker Services](./images/docker_services_02.png "Spark Docker Services")

Minio UI
```
http://localhost:9001/browser/delta-bucket
```

![Minio Delta Table](./images/minio_delta_table.png "Minio Delta Table")

Spark Master UI
```
http://localhost:8080/
```

![Spark Master UI](./images/spark_master.png "Spark Master UI")

Spark Worker UI
```
http://localhost:8081/
```

![Spark Worker UI](./images/spark_worker.png "Spark Worker UI")

## Notes
Actually exist a `spark-client` [python package](https://pypi.org/project/pyspark-client/) implement only Spark Connect, but the minimum version is 4.0.0. Unistalling pyspark 3.5.0 and use this light version could works. But you must refactor the Dockerfile to get the compatible jar files with this new Spark Connect 4.0.0 version.

## Links 
- [Spark Docker Hub](https://hub.docker.com/_/spark)
- [spark Session Configuration ](https://spark.apache.org/docs/latest/configuration.html)