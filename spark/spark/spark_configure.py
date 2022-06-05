import json
import logging
import os

import boto3
from pyspark import SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from utils.my_secret import profile_info
from utils.utils import BUCKET_NAME, REGION


class SparkResult(object):
    def __init__(self):
        self.spark = SparkSession.builder.appName("spark_result").getOrCreate()

    def create_DF(self, schema):  # -> pd.DataFrame:
        emptyRDD = self.spark.sparkContext.emptyRDD()
        return self.spark.createDataFrame(emptyRDD, schema=schema)


class SparkS3(object):
    # 기본으로 가져감
    conf = SparkConf()

    conf.set("spark.executor.instances", "8")
    conf.set("spark.executor.cores", 2)
    conf.set("spark.shuffle.compress", "true")

    conf.set("spark.hadoop.fs.s3a.access.key", profile_info["aws_access_key_id"])
    conf.set("spark.hadoop.fs.s3a.secret.key", profile_info["aws_secret_access_key"])
    conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{profile_info['region']}.amazonaws.com")

    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    conf.set("spark.hadoop.com.amazonaws.services.s3.enableV2", "true")
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    # # S3 REGION 설정 ( V4 때문에 필요 )
    # conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")

    # 용량이 커서 설정해야 함
    conf.set("spark.hadoop.fs.s3a.multipart.size", 104857600)
    conf.set("spark.sql.debug.maxToStringFields", 1000)

    # conf.set("spark.speculation", False)

    spark = None
    bucket_name = BUCKET_NAME
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=profile_info["aws_access_key_id"],
        aws_secret_access_key=profile_info["aws_secret_access_key"],
    )

    def __init__(self):
        logging.error("spark start")
        self.spark = (
            SparkSession.builder.config(conf=self.conf)
            .config("fs.s3a.access.key", profile_info["aws_access_key_id"])
            .config("fs.s3a.access.key", profile_info["aws_secret_access_key"])
            .appName("RTC_SPARK")
            .master("local[*]")
            .getOrCreate()
        )

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        # self.spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
        # self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", profile_info["aws_access_key_id"])
        # self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", profile_info["aws_secret_access_key"])
        # self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        # self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    def stop_spark(self):
        self.spark.stop()

    # def get_file(self, file_name="convert_code.csv") -> DataFrame:
    #     try:
    #         file_name = f"s3a://{self.bucket_name}/{file_name}"
    #         print(f"trying {file_name}")
    #         df_spark = (
    #             self.spark.read.format("csv")
    #             .option("encoding", "euc-kr")
    #             .option("header", True)
    #             .option("inferSchema", False)
    #             .load(file_name)
    #             .coalesce(1)
    #         )

    #         return df_spark
    #     except:
    #         logging.error("no file exists")
    #         return None

    def send_file(self, save_df_result: DataFrame, key: str):
        try:
            save_df_result.write.option("header", "true").csv("s3a://{BUCKET_NAME}/{key}")
            print("send file by hadoop")

        except:
            self.spark.s3_client.put_object(
                Body=json.dumps(save_df_result), Bucket=BUCKET_NAME, Key=key,
            )
            print("send file by boto3")

    def get_file(self, file_name="test_three.csv") -> DataFrame:
        try:
            file_path = f"Downloads/{file_name}"

            if not os.path.exists("Downloads"):
                os.mkdir("Downloads")

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

            return df_spark
        except:
            logging.error("no file exists")
            return None

