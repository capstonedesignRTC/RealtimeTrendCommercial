import json
import logging
import os

from pyspark import SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from utils.my_secret import profile_info


class SparkResult(object):
    def __init__(self):
        self.spark = SparkSession.builder.appName("spark_result").getOrCreate()

    def create_DF(self, schema):  # -> pd.DataFrame:
        emptyRDD = self.spark.sparkContext.emptyRDD()
        return self.spark.createDataFrame(emptyRDD, schema=schema)


class SparkS3(object):
    # 기본으로 가져감
    conf = SparkConf()
    conf.set("spark.driver.memory", "14g")
    conf.set("spark.executor.memory", "14g")
    conf.set("spark.shuffle.compress", "true")
    conf.set("spark.sql.shuffle.partitions", 100)

    conf.set("spark.executor.instances", "8")  # num-executors
    conf.set("spark.executor.cores", "3")

    conf.set("spark.shuffle.compress", "true")
    conf.set("spark.sql.shuffle.partitions", "5")
    conf.set("spark.dynamicAllocation.enabled", "true")

    conf.set("spark.hadoop.fs.s3a.access.key", profile_info["aws_access_key_id"])
    conf.set("spark.hadoop.fs.s3a.secret.key", profile_info["aws_secret_access_key"])

    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.hadoop.com.amazonaws.services.s3.enableV2", "true")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)

    # 용량이 커서 설정해야 함
    conf.set("spark.hadoop.fs.s3a.multipart.size", 104857600)
    conf.set("spark.sql.debug.maxToStringFields", 1000)

    # conf.set("spark.speculation", False)

    spark = None

    send_bucket = "rtc-raw-data"

    def __init__(self):
        logging.error("spark start")
        self.spark = SparkSession.builder.config(conf=self.conf).appName("RTC_SPARK").master("local[*]").getOrCreate()

    def stop_spark(self):
        self.spark.stop()

    def get_file(self, file_name="convert_code.csv") -> DataFrame:
        try:
            file_name = f"s3://{self.send_bucket}/{file_name}"

            print(f"trying {file_name}")
            df_spark = (
                self.spark.read.format("csv")
                .option("encoding", "euc-kr")
                .option("header", True)
                .option("inferSchema", False)
                .load(file_name)
                .coalesce(1)
            )
            return df_spark
        except Exception as e:
            logging.error(e.__str__())
            return None
