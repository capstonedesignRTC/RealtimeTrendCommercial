FROM ubuntu:18.04

MAINTAINER red131729@khu.ac.kr

ARG spark_version=3.1.1
ARG DYNAMIC_JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ARG HADOOP_HOME=/opt/hadoop/hadoop-3.2.3
ARG SPARK_HOME=/opt/spark/spark-3.1.3-bin-hadoop3.2.tgz

## java
RUN apt-get update -y \
    &&  apt-get install wget -y \
    &&  apt-get install vim unzip ssh openjdk-8-jdk -y \
    &&  apt-get install supervisor -y

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME
ENV JAVA_HOME=${DYNAMIC_JAVA_HOME}

RUN apt-get update -y && \ 
    apt-get install -y python3 && \
    apt-get install -y python3-pip 

RUN pip3 install --upgrade pip setuptools wheel &&\
    pip3 install pyspark &&\
    pip3 install wget pyspark==${spark_version}

### spark
RUN mkdir /opt/spark && cd /opt/spark && \
    wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.1.3-bin-hadoop3.2.tgz && \
    rm spark-3.1.3-bin-hadoop3.2.tgz 

ENV SPARK_HOME=/opt/spark/spark-3.1.3-bin-hadoop3.2
RUN export SPARK_HOME
ENV PATH=${SPARK_HOME}/bin:$PATH
ENV PYSPARK_PYTHON=/usr/bin/python3

### hadoop
RUN mkdir /opt/hadoop && \
    cd /opt/hadoop && \
    wget https://dlcdn.apache.org/hadoop/common/hadoop-3.2.3/hadoop-3.2.3.tar.gz && \
    tar -xzf hadoop-3.2.3.tar.gz && \
    rm hadoop-3.2.3.tar.gz 

ENV HADOOP_HOME /opt/hadoop/hadoop-3.2.3
RUN export HADOOP_HOME
RUN echo ${HADOOP_HOME}
ENV PATH=${HADOOP_HOME}/bin:$PATH


### jar downloads
# aws-java-sdk 1.7.4
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    mv aws-java-sdk-bundle-1.11.901.jar ${SPARK_HOME}/jars

# hadoop-aws 2.7.3
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.3/hadoop-aws-3.2.3.jar && \
    mv hadoop-aws-3.2.3.jar ${SPARK_HOME}/jars

WORKDIR /home
COPY . .

RUN pip3 install -r requirements.txt
CMD [ "python3", "main.py" ]