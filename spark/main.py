import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# aws_client = boto3.client(
#     service_name="s3",
#     region_name="ap-northeast-2",
#     aws_access_key_id="AKIAZJ5G7MQHGFJITW6Z",
#     aws_secret_access_key="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",
# )


# # aws_client.download_file(Bucket="rtc25", Key=f"test.csv", Filename=f"test.csv")

###
spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

csv_path = "서울시 우리마을가게 상권분석서비스(상권배후지-생활인구).csv"
df_spark = spark.read.option("encoding", "euc-kr").option("header", True).csv(csv_path, header=True)


def seoul_three(df_spark) -> pd.DataFrame():
    # http://data.seoul.go.kr/dataList/OA-15582/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권배후지-생활인구)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 인구수
        col("총_생활인구_수").alias("TOT_FLPOP_CO"),
        col("남성_생활인구_수").alias("ML_FLPOP_CO"),
        col("여성_생활인구_수").alias("FML_FLPOP_CO"),
        # 연령대
        col("연령대_10_생활인구_수").alias("AGRDE_10_FLPOP_CO"),
        col("연령대_20_생활인구_수").alias("AGRDE_20_FLPOP_CO"),
        col("연령대_30_생활인구_수").alias("AGRDE_30_FLPOP_CO"),
        col("연령대_40_생활인구_수").alias("AGRDE_40_FLPOP_CO"),
        col("연령대_50_생활인구_수").alias("AGRDE_50_FLPOP_CO"),
        col("연령대_60_이상_생활인구_수").alias("AGRDE_60_ABOVE_FLPOP_CO"),
        # 요일
        col("월요일_생활인구_수").alias("MON_FLPOP_CO"),
        col("화요일_생활인구_수").alias("TUES_FLPOP_CO"),
        col("수요일_생활인구_수").alias("WED_FLPOP_CO"),
        col("목요일_생활인구_수").alias("THUR_FLPOP_CO"),
        col("금요일_생활인구_수").alias("FRI_FLPOP_CO"),
        col("토요일_생활인구_수").alias("SAT_FLPOP_CO"),
        col("일요일_생활인구_수").alias("SUN_FLPOP_CO"),
    )

    # df_spark.show(2)
    return df_spark


new_one = seoul_three(df_spark)
new_one.show(2)
