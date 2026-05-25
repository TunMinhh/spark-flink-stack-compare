FROM apache/flink:1.19-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==1.19.0 pyiceberg[s3fs]==0.7.1

# Install Hadoop binary — Iceberg's FlinkCatalogFactory.clusterHadoopConf()
# requires org.apache.hadoop.conf.Configuration on the classpath, even when
# using REST catalog with no HDFS.
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
# Try the active mirror first (fast), fall back to the archive (slow but always available).
RUN (wget --tries=3 --timeout=60 --waitretry=10 \
        "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -O /tmp/hadoop.tar.gz \
     || wget --tries=5 --timeout=180 --waitretry=15 \
        "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -O /tmp/hadoop.tar.gz) && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz
ENV HADOOP_CLASSPATH="${HADOOP_HOME}/etc/hadoop:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/yarn:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*"
ENV PATH=$PATH:${HADOOP_HOME}/bin

# Download all Flink connectors in one layer.
# --tries=5 --timeout=60 --waitretry=15: retries up to 5× with 60s connect
# timeout and 15s between retries — handles slow / flaky networks.
RUN wget -q --tries=5 --timeout=60 --waitretry=15 -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar" && \
    wget -q --tries=5 --timeout=60 --waitretry=15 -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar" && \
    wget -q --tries=5 --timeout=60 --waitretry=15 -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar" && \
    wget -q --tries=5 --timeout=60 --waitretry=15 -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar" && \
    wget -q --tries=5 --timeout=60 --waitretry=15 -P /opt/flink/lib/ \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

# Flink's checkpoint storage needs an S3 filesystem plugin. The apache/flink
# Docker entrypoint activates plugins listed in ENABLE_BUILT_IN_PLUGINS at
# container startup — set it via env in docker-compose to avoid classloader
# issues with manual file copies.

WORKDIR /opt/flink/work-dir
