import logging

from pyspark import SparkConf, SQLContext
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
    # 기본으로 가져감
    conf = SparkConf()
    # AWS ACCESS KEY 설정
    conf.set("spark.hadoop.fs.s3a.access.key", profile_info["aws_access_key_id"])
    conf.set("spark.hadoop.fs.s3a.secret.key", profile_info["aws_secret_access_key"])

    # S3 REGION 설정 ( V4 때문에 필요 )
    conf.set("spark.hadoop.fs.s3a.endpoint", f"s3.{profile_info['region']}.amazonaws.com")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # conf.set("spark.hadoop.fs.s3a.path.style.access", True)

    # 용량이 커서 설정해야 함
    conf.set("spark.hadoop.fs.s3a.multipart.size", 104857600)
    spark = None
    bucket_name = profile_info["aws_bucket_name"]

    def __init__(self, appname="RTC_SPARK"):
        logging.error("spark start")
        self.spark = SparkSession.builder.config(conf=self.conf).appName(appname).master("local[*]").getOrCreate()
        # spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", True)

    def stop_spark(self):
        self.spark.stop()

    def get_file(self, file_name="test_three.csv") -> DataFrame:
        try:
            file_name = f"s3a://{self.bucket_name}/{file_name}"
            df_spark = (
                self.spark.read.format("csv")
                .option("encoding", "euc-kr")
                .option("header", True)
                .option("inferSchema", True)
                .csv(file_name)
                .coalesce(1)
            )
            return df_spark
        except:
            logging.error("no file exists")
            return None

    def get_file_test(self, local_file_path="utils/test_three.csv") -> DataFrame:
        df_spark = (
            self.spark.read.format("csv")
            .option("encoding", "euc-kr")
            .option("header", True)
            .option("inferSchema", True)
            .csv(local_file_path)
            .coalesce(1)
        )

        return df_spark

