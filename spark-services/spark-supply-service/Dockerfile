FROM spark:3.5.0-python3

USER root
RUN apt-get update
RUN apt-get install -y curl

# all python spark dependencies (we must add pyspark too because delta spark install newer versions not compatible with scala)
RUN pip install delta-spark==3.1.0 pyspark==3.5.0 pandas tables

# all java spark dependencies
USER spark
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar --output /opt/spark/jars/delta-spark_2.12-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar -o /opt/spark/jars/delta-storage-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.508/aws-java-sdk-bundle-1.12.508.jar --output /opt/spark/jars/aws-java-sdk-bundle-1.12.508.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar --output /opt/spark/jars/hadoop-aws-3.3.4.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar --output /opt/spark/jars/hadoop-common-3.3.4.jar
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar --output /opt/spark/jars/hadoop-auth-3.3.4.jar