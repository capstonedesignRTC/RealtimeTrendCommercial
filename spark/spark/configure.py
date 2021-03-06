import json
import logging
import os
from time import sleep

import boto3
from pyspark import SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from utils.my_secret import profile_info


class SparkResult(object):
    def __init__(self):
        self.spark = SparkSession.builder.appName("spark_result").getOrCreate()

    def create_DF(self, schema) -> DataFrame:
        emptyRDD = self.spark.sparkContext.emptyRDD()
        return self.spark.createDataFrame(emptyRDD, schema=schema)


class SparkS3(object):
    conf = SparkConf()
    conf.set("spark.driver.memory", "14g")
    conf.set("spark.executor.memory", "14g")

    conf.set("spark.sql.shuffle.partitions", "100")
    conf.set("spark.executor.instances", "8")
    conf.set("spark.executor.cores", "4")

    conf.set("spark.jars", "/opt/spark/spark-3.1.3-bin-hadoop3.2/jars/hadoop-aws-3.2.3.jar")
    # conf.set("spark.jars", "/opt/spark/jars/hadoop-aws-3.2.3.jar")

    conf.set("spark.shuffle.compress", "true")
    conf.set("spark.io.compress.codec", "org.apache.spark.io.LZFCompressionCodec")
    conf.set("spark.dynamicAllocation.enabled", "true")

    conf.set("spark.hadoop.fs.s3a.access.key", profile_info["aws_access_key_id"])
    conf.set("spark.hadoop.fs.s3a.secret.key", profile_info["aws_secret_access_key"])
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.hadoop.com.amazonaws.services.s3.enableV2", "true")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    conf.set("spark.hadoop.fs.s3a.multipart.size", 104857600)
    conf.set("spark.sql.debug.maxToStringFields", 1000)

    spark = None

    get_bucket = "rtc-raw-data"
    send_bucket = "rtc-result"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=profile_info["aws_access_key_id"],
        aws_secret_access_key=profile_info["aws_secret_access_key"],
    )

    def __init__(self):
        logging.error("spark start")
        self.spark = SparkSession.builder.config(conf=self.conf).appName("RTC_SPARK").master("local[*]").getOrCreate()
        sleep(1)

    def stop_spark(self):
        self.spark.stop()

    def send_file(self, save_df_result: DataFrame, key: str):
        if not os.path.exists("Results"):
            os.mkdir("Results")

        try:
            # s3??? ????????? json ????????? ?????????
            df = save_df_result.toJSON().map(lambda x: json.loads(x)).collect()  # .repartition(1)
            self.s3_client.put_object(
                Body=json.dumps(df), Bucket=self.send_bucket, Key=f"test/{key}",
            )
        except Exception as e:
            print(e.__str__())
        try:
            save_df_result.write.format("org.apache.spark.sql.json").mode("append").save(
                f"s3://{self.send_bucket}/test/{key}"
            )
            save_df_result.write.format("json").save(f"s3://{self.send_bucket}/test/f_{key}")
        except Exception as e:
            print(e.__str__())

    def get_file(self, file_name="convert_code.csv") -> DataFrame:
        try:
            file_name = f"s3://{self.get_bucket}/{file_name}"

            print(f"try fetching {file_name}")
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

    def get_file_by_boto3(self, file_name="test_three.csv") -> DataFrame:
        try:
            file_path = f"Downloads/{file_name}"

            if not os.path.exists("Downloads"):
                os.mkdir("Downloads")

            if not os.path.exists(file_path):
                self.s3_client.download_file(Bucket=self.get_bucket, Key=file_name, Filename=file_path)

            df_spark = (
                self.spark.read.format("csv")
                .option("encoding", "euc-kr")
                .option("header", True)
                .option("inferSchema", True)
                .csv(file_path)
                .coalesce(1)
            )

            return df_spark
        except Exception as e:
            logging.error("no file exists")
            return None
