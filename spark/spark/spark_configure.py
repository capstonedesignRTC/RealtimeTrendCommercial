import logging
import os

import boto3
from pyspark import SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from utils.my_secret import profile_info

# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# REGION = os.getenv("REGION")
# AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")


class SparkResult(object):
    def __init__(self):
        self.spark = SparkSession.builder.appName("spark_result").getOrCreate()

    def create_DF(self, schema):  # -> pd.DataFrame:
        emptyRDD = self.spark.sparkContext.emptyRDD()
        return self.spark.createDataFrame(emptyRDD, schema=schema)


class SparkS3(object):
    # 기본으로 가져감
    conf = SparkConf()

    # AWS ACCESS KEY 설정
    conf.set("spark.hadoop.fs.s3a.access.key", profile_info["aws_access_key_id"])
    conf.set("spark.hadoop.fs.s3a.secret.key", profile_info["aws_secret_access_key"])

    # S3 REGION 설정 ( V4 때문에 필요 )
    conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{profile_info['region']}.amazonaws.com")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access", True)

    # 용량이 커서 설정해야 함
    conf.set("spark.hadoop.fs.s3a.multipart.size", 104857600)
    conf.set("spark.sql.debug.maxToStringFields", 1000)

    # conf.set("log4j.logger.org.apache.hadoop.metrics2", "WARN")
    # conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    # conf.set("spark.speculation", False)

    spark = None
    bucket_name = profile_info["aws_bucket_name"]
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=profile_info["aws_access_key_id"],
        aws_secret_access_key=profile_info["aws_secret_access_key"],
    )

    def __init__(self, appname="RTC_SPARK"):
        logging.error("spark start")
        self.spark = SparkSession.builder.config(conf=self.conf).appName(appname).master("local[*]").getOrCreate()
        self.spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        self.spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    def stop_spark(self):
        self.spark.stop()

    def get_file(self, file_name="test_three.csv") -> DataFrame:
        try:
            file_path = f"Downloads/{file_name}"

            if not os.path.exists(file_path):
                self.s3_client.download_file(Bucket=self.bucket_name, Key=file_name, Filename=file_path)

            df_spark = (
                self.spark.read.format("csv")
                .option("encoding", "euc-kr")  #  "utf-8")
                .option("header", True)
                .option("inferSchema", True)
                .csv(file_path)
                .coalesce(1)
            )

            # df_spark = df_spark.limit(10)
            # os.remove(file_path)
            return df_spark
        except:
            logging.error("no file exists")
            return None

    def get_file_s3a(self, file_name="convert_code.csv") -> DataFrame:
        try:
            file_name = f"s3a://{self.bucket_name}/{file_name}"

            df_spark = (
                self.spark.read.format("csv")
                .option("encoding", "utf-8")  # "euc-kr"
                .option("header", True)
                .option("inferSchema", False)
                .load(file_name)
                .coalesce(1)
            )

            return df_spark
        except:
            logging.error("no file exists")
            return None
