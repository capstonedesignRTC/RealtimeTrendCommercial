import pandas
from pyspark import SparkConf, SQLContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from my_secret import profile_info


def get_spark(appname="RTC_SPARK"):
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

    spark = SparkSession.builder.config(conf=conf).appName(appname).master("local[*]").getOrCreate()
    # spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", True)

    return spark


def get_file(spark, file_url="s3a://open-rtc-data/test_three.csv") -> pandas.DataFrame:
    df_spark = (
        spark.read.format("csv")
        .option("encoding", "euc-kr")
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_url)
        .coalesce(1)
    )
    return df_spark
