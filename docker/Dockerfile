# https://dev.to/mvillarrealb/creating-a-spark-standalone-cluster-with-docker-and-docker-compose-2021-update-6l4

# The builder step is used to download and configure the spark environment.
# NOTE: We use openjdk 11.0.5 instead of 18.0.2 because Delta Lake requires the earlier version.
# When we try to use openjdk 18.0.2, attempting to use Delta Lake throws the following error:
#   java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x4d4a9e37) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x4d4a9e37
FROM openjdk:11.0.5-jdk-slim-buster as builder

# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools netcat ca-certificates python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Fix the value of PYTHONHASHSEED.  This is needed when you use Python 3.3 or greater.
# NOTE: We use Spark version 3.2.2 because Delta Lake doesn't work correctly with 3.3.0.
ENV SPARK_VERSION=3.2.2 \
DELTA_VERSION=2.0.0 \
HADOOP_VERSION=3.2 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

#RUN echo "spark.jars.packages io.delta:delta-core_2.12:2.0.0" >> /opt/spark/conf/spark-defaults.conf
#RUN echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> /opt/spark/conf/spark-defaults.conf
#RUN echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> /opt/spark/conf/spark-defaults.conf

RUN pip3 install pyspark==${SPARK_VERSION} delta-spark==${DELTA_VERSION}

# Apache spark environment
FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 6066

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY src/start-spark.sh /
RUN chmod 755 /start-spark.sh

CMD ["/bin/bash", "/start-spark.sh"]
