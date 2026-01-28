# Description
Integrate Minio with Spark Delta Lake and Spark Connect. Test with a Spark Driver

# Steps

You must start these services

Start Minio Service
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
  server /data --console-address ":9001"
```

Start Spark Master Service
```
$ docker run -d \
  --name spark-master \
  --network spark-net \
  -p 7077:7077 \
  -p 8080:8080 \
  -p 15002:15002 \
  -v ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
  spark:3.5.0-python3-connect \
  bash -c "
    /opt/spark/sbin/start-master.sh && \
    /opt/spark/sbin/start-connect-server.sh && \
    tail -f /opt/spark/logs/*
  "
```

Start Spark Worker Service
```
$ docker run -d \
    --name spark-worker \
    --network spark-net \
    -p 8081:8081 \
    -v ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
    spark:3.5.0-python3 \
    bash -c "
      /opt/spark/sbin/start-worker.sh \
      spark://spark-master:7077 && \
      tail -f /opt/spark/logs/*
    "
```