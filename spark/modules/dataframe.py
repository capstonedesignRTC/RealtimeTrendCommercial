import json
import logging
import time

import pyspark.sql.functions as F
from pyspark import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (col, lit, monotonically_increasing_id, udf,
                                   when)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spark.spark_configure import SparkResult

from modules.utils import CONVERT_CODE


def get_big_code(code):
    code = str(code)
    for big, val1 in CONVERT_CODE.items():
        for middle, val2 in val1.items():
            for small in val2.keys():
                if small == code:
                    return int(middle)

    return None


def get_new_columns(list_vals):
    return [get_big_code(k) for k in list_vals]


class DF(object):
    schema = StructType(
        [
            StructField("STDR_YY_CD", IntegerType(), nullable=False),
            StructField("STDR_QU_CD", IntegerType(), nullable=False),
            # StructField("SIGNGU_CD", IntegerType(), nullable=True),
            StructField("ADSTRD_CD", StringType(), nullable=True),
            # StructField("TRDAR_CD", IntegerType(), nullable=True),
        ]
    )

    def __init__(self):
        self.spark = SparkResult()

    def get_empty_dataframe(self) -> DataFrame:
        df = self.spark.create_DF(self.schema)
        return df

    def get_function(self, num: int):
        if num == 1:
            return self.seoul_one, self.calculate_one
        elif num == 2:
            return self.seoul_two, self.calculate_two
        elif num == 3:
            return self.seoul_three, self.calculate_three
        elif num == 4:
            return self.seoul_four, self.calculate_four
        elif num == 5:
            return self.seoul_five, self.calculate_five
        elif num == 6:
            return self.seoul_six, self.calculate_six
        elif num == 7:
            return self.seoul_seven, self.calculate_seven
        elif num == 8:
            return self.seoul_eight, self.calculate_eight
        elif num == 10:
            return self.seoul_ten, self.calculate_ten
        elif num == 11:
            return self.seoul_eleven, self.calculate_eleven
        elif num == 14:
            return self.seoul_fourteen, self.calculate_fourteen
        elif num == 15:
            return self.seoul_fifteen, self.calculate_fifteen

    def seoul_one(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15568/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-생활인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준 년코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 인구수
            col("TOT_FLPOP_CO"),  # col("총_생활인구_수").alias("TOT_FLPOP_CO"),
            col("ML_FLPOP_CO"),  # col("남성_생활인구_수").alias("ML_FLPOP_CO"),
            col("FML_FLPOP_CO"),  # col("여성_생활인구_수").alias("FML_FLPOP_CO"),
            # 연령대
            # col("AGRDE_10_FLPOP_CO"),  # col("연령대_10_생활인구_수").alias("AGRDE_10_FLPOP_CO"),
            col("AGRDE_20_FLPOP_CO"),  # col("연령대_20_생활인구_수").alias("AGRDE_20_FLPOP_CO"),
            col("AGRDE_30_FLPOP_CO"),  # col("연령대_30_생활인구_수").alias("AGRDE_30_FLPOP_CO"),
            col("AGRDE_40_FLPOP_CO"),  # col("연령대_40_생활인구_수").alias("AGRDE_40_FLPOP_CO"),
            # col("AGRDE_50_FLPOP_CO"),  # col("연령대_50_생활인구_수").alias("AGRDE_50_FLPOP_CO"),
            col("AGRDE_60_ABOVE_FLPOP_CO"),  # col("연령대_60_이상_생활인구_수").alias("AGRDE_60_ABOVE_FLPOP_CO"),
            # 요일
            # col("MON_FLPOP_CO"),  # col("월요일_생활인구_수").alias("MON_FLPOP_CO"),
            # col("TUES_FLPOP_CO"),  # col("화요일_생활인구_수").alias("TUES_FLPOP_CO"),
            # col("WED_FLPOP_CO"),  # col("수요일_생활인구_수").alias("WED_FLPOP_CO"),
            # col("THUR_FLPOP_CO"),  # col("목요일_생활인구_수").alias("THUR_FLPOP_CO"),
            # col("FRI_FLPOP_CO"),  # col("금요일_생활인구_수").alias("FRI_FLPOP_CO"),
            col("SAT_FLPOP_CO"),  # col("토요일_생활인구_수").alias("SAT_FLPOP_CO"),
            col("SUN_FLPOP_CO"),  # col("일요일_생활인구_수").alias("SUN_FLPOP_CO"),
        )

        # df_spark.show(8)
        return df_spark

    def calculate_one(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_one start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        총 인구, 20대, 토요일, 일요일
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TOT_FLPOP_CO"),
            col("AGRDE_20_FLPOP_CO"),
            col("SAT_FLPOP_CO"),
            col("SUN_FLPOP_CO"),  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        )
        df_one = df_data_one.orderBy(  # 애초에 정렬을 다음과 같이 진행
            col("TOT_FLPOP_CO").desc(),
            col("AGRDE_20_FLPOP_CO").desc(),
            col("SAT_FLPOP_CO").desc(),
            col("SUN_FLPOP_CO").desc(),
        )

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        여성 인구, 30대, 40대, 총_생활인구_수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("FML_FLPOP_CO"),
            col("AGRDE_30_FLPOP_CO"),
            col("AGRDE_40_FLPOP_CO"),
            col("TOT_FLPOP_CO"),  # 기준 년 코드  # 기준 분기 코드
        )
        df_two = df_data_two.orderBy(
            col("FML_FLPOP_CO").desc(),
            col("AGRDE_30_FLPOP_CO").desc(),
            col("AGRDE_40_FLPOP_CO").desc(),
            col("TOT_FLPOP_CO").desc(),
        )

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        df_one.drop()
        df_two.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )
        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK1"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")

        return result

    def seoul_two(self, df_spark: DataFrame) -> DataFrame:
        # 서울시 우리마을가게 상권분석서비스(상권-직장인구)
        # https://data.seoul.go.kr/dataList/OA-15569/S/1/datasetView.do
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년월_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 인구수
            col("TOT_WRC_POPLTN_CO"),  # col("총_직장_인구_수").alias("TOT_WRC_POPLTN_CO"),
            col("ML_WRC_POPLTN_CO"),  # col("남성_직장_인구_수").alias("ML_WRC_POPLTN_CO"),
            # col("FML_WRC_POPLTN_CO"),  # col("여성_직장_인구_수").alias("FML_WRC_POPLTN_CO"),
            # 연령대
            # col("AGRDE_10_WRC_POPLTN_CO"),  # col("연령대_10_직장_인구_수").alias("AGRDE_10_WRC_POPLTN_CO"),
            # col("AGRDE_20_WRC_POPLTN_CO"),  # col("연령대_20_직장_인구_수").alias("AGRDE_20_WRC_POPLTN_CO"),
            col("AGRDE_30_WRC_POPLTN_CO"),  # col("연령대_30_직장_인구_수").alias("AGRDE_30_WRC_POPLTN_CO"),
            col("AGRDE_40_WRC_POPLTN_CO"),  # col("연령대_40_직장_인구_수").alias("AGRDE_40_WRC_POPLTN_CO"),
            col("AGRDE_50_WRC_POPLTN_CO"),  # col("연령대_50_직장_인구_수").alias("AGRDE_50_WRC_POPLTN_CO"),
            # col("AGRDE_60_ABOVE_WRC_POPLTN_CO"),  # col("연령대_60_이상_직장_인구_수").alias("AGRDE_60_ABOVE_WRC_POPLTN_CO"),
        )

        return df_spark

    def calculate_two(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_two start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None
        """
        방법 1
        총_직장_인구_수, 연령대_30_직장_인구_수, 연령대_40_직장_인구_수
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TOT_WRC_POPLTN_CO"),
            col("AGRDE_30_WRC_POPLTN_CO"),
            col("AGRDE_40_WRC_POPLTN_CO"),  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        )
        df_one = df_data_one.orderBy(
            col("TOT_WRC_POPLTN_CO").desc(), col("AGRDE_30_WRC_POPLTN_CO").desc(), col("AGRDE_40_WRC_POPLTN_CO").desc(),
        )

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        남성_직장_인구_수, 연령대_40_직장_인구_수, 연령대_50_직장_인구_수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("ML_WRC_POPLTN_CO"),
            col("AGRDE_40_WRC_POPLTN_CO"),
            col("AGRDE_50_WRC_POPLTN_CO"),  # 기준 년 코드  # 기준 분기 코드
        )
        df_two = df_data_two.orderBy(
            col("ML_WRC_POPLTN_CO").desc(), col("AGRDE_40_WRC_POPLTN_CO").desc(), col("AGRDE_50_WRC_POPLTN_CO").desc(),
        )

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        df_one.drop()
        df_two.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )
        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK2"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_three(self, df_spark: DataFrame) -> DataFrame:
        # http://data.seoul.go.kr/dataList/OA-15582/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권배후지-생활인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 인구수
            col("TOT_FLPOP_CO"),  # col("총_생활인구_수").alias("TOT_FLPOP_CO"),
            col("ML_FLPOP_CO"),  # col("남성_생활인구_수").alias("ML_FLPOP_CO"),
            col("FML_FLPOP_CO"),  # col("여성_생활인구_수").alias("FML_FLPOP_CO"),
            # 연령대
            # col("AGRDE_10_FLPOP_CO"),  # col("연령대_10_생활인구_수").alias("AGRDE_10_FLPOP_CO"),
            col("AGRDE_20_FLPOP_CO"),  # col("연령대_20_생활인구_수").alias("AGRDE_20_FLPOP_CO"),
            col("AGRDE_30_FLPOP_CO"),  # col("연령대_30_생활인구_수").alias("AGRDE_30_FLPOP_CO"),
            col("AGRDE_40_FLPOP_CO"),  # col("연령대_40_생활인구_수").alias("AGRDE_40_FLPOP_CO"),
            # col("AGRDE_50_FLPOP_CO"),  # col("연령대_50_생활인구_수").alias("AGRDE_50_FLPOP_CO"),
            # col("AGRDE_60_ABOVE_FLPOP_CO"),  # col("연령대_60_이상_생활인구_수").alias("AGRDE_60_ABOVE_FLPOP_CO"),
            # 요일
            # col("MON_FLPOP_CO"),  # col("월요일_생활인구_수").alias("MON_FLPOP_CO"),
            # col("TUES_FLPOP_CO"),  # col("화요일_생활인구_수").alias("TUES_FLPOP_CO"),
            # col("WED_FLPOP_CO"),  # col("수요일_생활인구_수").alias("WED_FLPOP_CO"),
            # col("THUR_FLPOP_CO"),  # col("목요일_생활인구_수").alias("THUR_FLPOP_CO"),
            # col("FRI_FLPOP_CO"),  # col("금요일_생활인구_수").alias("FRI_FLPOP_CO"),
            col("SAT_FLPOP_CO"),  # col("토요일_생활인구_수").alias("SAT_FLPOP_CO"),
            col("SUN_FLPOP_CO"),  # col("일요일_생활인구_수").alias("SUN_FLPOP_CO"),
        )

        return df_spark

    def calculate_three(self, df_data: DataFrame, quarter: int = 1):  # , big: int, middle: int, small: int):
        logging.error("calculate_three start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None
        """
        방법 1
        총 인구, 20대, 토요일, 일요일
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TOT_FLPOP_CO"),
            col("AGRDE_20_FLPOP_CO"),
            col("SAT_FLPOP_CO"),
            col("SUN_FLPOP_CO"),  # 기준 년 코드  # 기준 분기 코드
        )
        df_one = df_data_one.orderBy(
            col("TOT_FLPOP_CO").desc(),
            col("AGRDE_20_FLPOP_CO").desc(),
            col("SAT_FLPOP_CO").desc(),
            col("SUN_FLPOP_CO").desc(),
        )

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        여성 인구, 30대, 40대, 총_생활인구_수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("FML_FLPOP_CO"),
            col("AGRDE_30_FLPOP_CO"),
            col("AGRDE_40_FLPOP_CO"),
            col("TOT_FLPOP_CO"),  # 기준 년 코드  # 기준 분기 코드
        )
        df_two = df_data_two.orderBy(
            col("FML_FLPOP_CO").desc(),
            col("AGRDE_30_FLPOP_CO").desc(),
            col("AGRDE_40_FLPOP_CO").desc(),
            col("TOT_FLPOP_CO").desc(),
        )

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        df_one.drop()
        df_two.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )
        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK3"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_four(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15570/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권배후지-직장인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 인구수
            col("TOT_WRC_POPLTN_CO"),  # col("총_직장_인구_수").alias("TOT_WRC_POPLTN_CO"),
            col("ML_WRC_POPLTN_CO"),  # col("남성_직장_인구_수").alias("ML_WRC_POPLTN_CO"),
            # col("FML_WRC_POPLTN_CO"),  # col("여성_직장_인구_수").alias("FML_WRC_POPLTN_CO"),
            # 연령대
            # col("AGRDE_10_WRC_POPLTN_CO"),  # col("연령대_10_직장_인구_수").alias("AGRDE_10_WRC_POPLTN_CO"),
            # col("AGRDE_20_WRC_POPLTN_CO"),  # col("연령대_20_직장_인구_수").alias("AGRDE_20_WRC_POPLTN_CO"),
            col("AGRDE_30_WRC_POPLTN_CO"),  # col("연령대_30_직장_인구_수").alias("AGRDE_30_WRC_POPLTN_CO"),
            col("AGRDE_40_WRC_POPLTN_CO"),  # col("연령대_40_직장_인구_수").alias("AGRDE_40_WRC_POPLTN_CO"),
            col("AGRDE_50_WRC_POPLTN_CO"),  # col("연령대_50_직장_인구_수").alias("AGRDE_50_WRC_POPLTN_CO"),
            # col("AGRDE_60_ABOVE_WRC_POPLTN_CO"),  # col("연령대_60_이상_직장_인구_수").alias("AGRDE_60_ABOVE_WRC_POPLTN_CO"),
        )

        return df_spark

    def calculate_four(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_two start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        총_직장_인구_수, 연령대_30_직장_인구_수, 연령대_40_직장_인구_수
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TOT_WRC_POPLTN_CO"),
            col("AGRDE_30_WRC_POPLTN_CO"),
            col("AGRDE_40_WRC_POPLTN_CO"),  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        )
        df_one = df_data_one.orderBy(
            col("TOT_WRC_POPLTN_CO").desc(), col("AGRDE_30_WRC_POPLTN_CO").desc(), col("AGRDE_40_WRC_POPLTN_CO").desc(),
        )

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        남성_직장_인구_수, 연령대_40_직장_인구_수, 연령대_50_직장_인구_수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("ML_WRC_POPLTN_CO"),
            col("AGRDE_40_WRC_POPLTN_CO"),
            col("AGRDE_50_WRC_POPLTN_CO"),  # 기준 년 코드  # 기준 분기 코드
        )
        df_two = df_data_two.orderBy(
            col("ML_WRC_POPLTN_CO").desc(), col("AGRDE_40_WRC_POPLTN_CO").desc(), col("AGRDE_50_WRC_POPLTN_CO").desc(),
        )

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        df_one.drop()
        df_two.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )
        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK4"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_five(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15584/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권_상주인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권 코드").alias("TRDAR_CD"),
            # 인구수
            col("TOT_REPOP_CO"),  # col("총 상주인구 수").alias("TOT_REPOP_CO"),
            # col("ML_REPOP_CO"),  # col("남성 상주인구 수").alias("ML_REPOP_CO"),
            # col("FML_REPOP_CO"),  # col("여성 상주인구 수").alias("FML_REPOP_CO"),
            # 연령대
            # col("AGRDE_10_REPOP_CO"),  # col("연령대 10 상주인구 수").alias("AGRDE_10_REPOP_CO"),
            # col("AGRDE_20_REPOP_CO"),  # col("연령대 20 상주인구 수").alias("AGRDE_20_REPOP_CO"),
            col("AGRDE_30_REPOP_CO"),  # col("연령대 30 상주인구 수").alias("AGRDE_30_REPOP_CO"),
            col("AGRDE_40_REPOP_CO"),  # col("연령대 40 상주인구 수").alias("AGRDE_40_REPOP_CO"),
            # col("AGRDE_50_REPOP_CO"),  # col("연령대 50 상주인구 수").alias("AGRDE_50_REPOP_CO"),
            # col("AGRDE_60_ABOVE_REPOP_CO"),  # col("연령대 60 이상 상주인구 수").alias("AGRDE_60_ABOVE_REPOP_CO"),
            # 총 가구 수
            col("TOT_HSHLD_CO"),  # col("총 가구 수").alias("TOT_HSHLD_CO"),
            col("APT_HSHLD_CO"),  # col("아파트 가구 수").alias("APT_HSHLD_CO"),
            col("NON_APT_HSHLD_CO"),  # col("비 아파트 가구 수").alias("NON_APT_HSHLD_CO"),
        )

        return df_spark

    def calculate_five(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_five start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        총 상주인구 수, 연령대 30 상주인구 수, 연령대 40 상주인구 수
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TOT_REPOP_CO"),
            col("AGRDE_30_REPOP_CO"),
            col("AGRDE_40_REPOP_CO"),
        )  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        df_one = df_data_one.orderBy(
            col("TOT_REPOP_CO").desc(), col("AGRDE_30_REPOP_CO").desc(), col("AGRDE_40_REPOP_CO").desc(),
        )

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        총 가구 수, 아파트 가구 수, 비 아파트 가구 수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TOT_HSHLD_CO"),
            col("APT_HSHLD_CO"),
            col("NON_APT_HSHLD_CO"),
        )  # 기준 년 코드  # 기준 분기 코드
        df_two = df_data_two.orderBy(
            col("TOT_HSHLD_CO").desc(), col("APT_HSHLD_CO").desc(), col("NON_APT_HSHLD_CO").desc(),
        )

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        df_one.drop()
        df_two.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )
        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK5"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )
        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_six(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-21278/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-소득소비)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 금액
            col("MT_AVRG_INCOME_AMT"),  # col("월_평균_소득_금액").alias("MT_AVRG_INCOME_AMT"),
            col("INCOME_SCTN_CD"),  # col("소득_구간_코드").alias("INCOME_SCTN_CD"),
            col("EXPNDTR_TOTAMT"),  # col("지출_총금액").alias("EXPNDTR_TOTAMT"),
            # col("FDSTFFS_EXPNDTR_TOTAMT"),  # col("식료품_지출_총금액").alias("FDSTFFS_EXPNDTR_TOTAMT"),
            # col("CLTHS_FTWR_EXPNDTR_TOTAMT"),  # col("의류_신발_지출_총금액").alias("CLTHS_FTWR_EXPNDTR_TOTAMT"),
            # col("LVSPL_EXPNDTR_TOTAMT"),  # col("생활용품_지출_총금액").alias("LVSPL_EXPNDTR_TOTAMT"),
            # col("MCP_EXPNDTR_TOTAMT"),  # col("의료비_지출_총금액").alias("MCP_EXPNDTR_TOTAMT"),
            # col("TRNSPORT_EXPNDTR_TOTAMT"),  # col("교통_지출_총금액").alias("TRNSPORT_EXPNDTR_TOTAMT"),
            # col("LSR_EXPNDTR_TOTAMT"),  # col("여가_지출_총금액").alias("LSR_EXPNDTR_TOTAMT"),
            # col("CLTUR_EXPNDTR_TOTAMT"),  # col("문화_지출_총금액").alias("CLTUR_EXPNDTR_TOTAMT"),
            # col("EDC_EXPNDTR_TOTAMT"),  # col("교육_지출_총금액").alias("EDC_EXPNDTR_TOTAMT"),
            # col("PLESR_EXPNDTR_TOTAMT"),  # col("유흥_지출_총금액").alias("PLESR_EXPNDTR_TOTAMT"),
        )

        return df_spark

    def calculate_six(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_six start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == 4)
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        월_평균_소득_금액, 지출_총금액, 소득_구간_코드
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("MT_AVRG_INCOME_AMT"),
            col("EXPNDTR_TOTAMT"),
            col("INCOME_SCTN_CD"),
        )
        df_one = df_data_one.orderBy(
            col("INCOME_SCTN_CD").asc(),
            col("EXPNDTR_TOTAMT").asc(),
            col("MT_AVRG_INCOME_AMT").asc()
            # col("MT_AVRG_INCOME_AMT").desc(), col("EXPNDTR_TOTAMT").desc(), col("INCOME_SCTN_CD").desc()
        )

        result = df_one.rdd.zipWithIndex().toDF()
        df_one.drop()
        result = result.select(col("_1.*"), col("_2").alias("RANK6"))
        result = result.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK6"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_seven(self, df_spark: DataFrame) -> DataFrame:
        # http://data.seoul.go.kr/dataList/OA-15572/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-추정매출)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            col("SVC_INDUTY_CD"),  #  col("서비스_업종_코드").alias("SVC_INDUTY_CD"),  # THSMON_SELNG_AMT,  THSMON_SELNG_CO
            # 비율
            col("MDWK_SELNG_RATE"),  # col("주중_매출_비율").alias("MDWK_SELNG_RATE"),
            col("WKEND_SELNG_RATE"),  # col("주말_매출_비율").alias("WKEND_SELNG_RATE"),
            # col("MON_SELNG_RATE"),  # col("월요일_매출_비율").alias("MON_SELNG_RATE"),
            # col("TUES_SELNG_RATE"),  # col("화요일_매출_비율").alias("TUES_SELNG_RATE"),
            # col("WED_SELNG_RATE"),  # col("수요일_매출_비율").alias("WED_SELNG_RATE"),
            # col("THUR_SELNG_RATE"),  # col("목요일_매출_비율").alias("THUR_SELNG_RATE"),
            # col("FRI_SELNG_RATE"),  # col("금요일_매출_비율").alias("FRI_SELNG_RATE"),
            # col("SAT_SELNG_RATE"),  # col("토요일_매출_비율").alias("SAT_SELNG_RATE"),
            # col("SUN_SELNG_RATE"),  # col("일요일_매출_비율").alias("SUN_SELNG_RATE"),
            col("ML_SELNG_RATE"),  # col("남성_매출_비율").alias("ML_SELNG_RATE"),
            col("FML_SELNG_RATE"),  # col("여성_매출_비율").alias("FML_SELNG_RATE"),
            # 금액
            col("MDWK_SELNG_AMT"),  # col("주중_매출_금액").alias("MDWK_SELNG_AMT"),
            col("WKEND_SELNG_AMT"),  # col("주말_매출_금액").alias("WKEND_SELNG_AMT"),
            # col("MON_SELNG_AMT"),  # col("월요일_매출_금액").alias("MON_SELNG_AMT"),
            # col("TUES_SELNG_AMT"),  # col("화요일_매출_금액").alias("TUES_SELNG_AMT"),
            # col("WED_SELNG_AMT"),  # col("수요일_매출_금액").alias("WED_SELNG_AMT"),
            # col("THUR_SELNG_AMT"),  # col("목요일_매출_금액").alias("THUR_SELNG_AMT"),
            # col("FRI_SELNG_AMT"),  # col("금요일_매출_금액").alias("FRI_SELNG_AMT"),
            # col("SAT_SELNG_AMT"),  # col("토요일_매출_금액").alias("SAT_SELNG_AMT"),
            # col("SUN_SELNG_AMT"),  # col("일요일_매출_금액").alias("SUN_SELNG_AMT"),
            col("ML_SELNG_AMT"),  # col("남성_매출_금액").alias("ML_SELNG_AMT"),
            col("FML_SELNG_AMT"),  # col("여성_매출_금액").alias("FML_SELNG_AMT"),
            # 건수
            col("MDWK_SELNG_CO"),  # col("주중_매출_건수").alias("MDWK_SELNG_CO"),
            col("WKEND_SELNG_CO"),  # col("주말_매출_건수").alias("WKEND_SELNG_CO"),
            # col("MON_SELNG_CO"),  # col("월요일_매출_건수").alias("MON_SELNG_CO"),
            # col("TUES_SELNG_CO"),  # col("화요일_매출_건수").alias("TUES_SELNG_CO"),
            # col("WED_SELNG_CO"),  # col("수요일_매출_건수").alias("WED_SELNG_CO"),
            # col("THUR_SELNG_CO"),  # col("목요일_매출_건수").alias("THUR_SELNG_CO"),
            # col("FRI_SELNG_CO"),  # col("금요일_매출_건수").alias("FRI_SELNG_CO"),
            # col("SAT_SELNG_CO"),  # col("토요일_매출_건수").alias("SAT_SELNG_CO"),
            # col("SUN_SELNG_CO"),  # col("일요일_매출_건수").alias("SUN_SELNG_CO"),
            col("ML_SELNG_CO"),  #  col("남성_매출_건수").alias("ML_SELNG_CO"),
            col("FML_SELNG_CO"),  # col("여성_매출_건수").alias("FML_SELNG_CO"),
        )

        return df_spark

    def calculate_seven(self, df_data: DataFrame, quarter: int = 1, service: str = "CS100001"):
        logging.error("calculate_seven start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.where((col("STDR_QU_CD") == quarter) & (col("SVC_INDUTY_CD") == service))
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        주중_매출_비율, 주말_매출_비율
        """

        df_data_one = df_data.select(
            col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("MDWK_SELNG_RATE"), col("WKEND_SELNG_RATE"),
        )  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        df_one = df_data_one.orderBy(col("MDWK_SELNG_RATE").desc(), col("WKEND_SELNG_RATE").desc(),)

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        주중_매출_금액 / 주중_매출_건수, 주말_매출_금액 / 주말_매출_건수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            (col("MDWK_SELNG_AMT") / col("MDWK_SELNG_CO")).alias("mdwk_amt_per_co"),  # # 컬럼끼리 나눠 계산
            (col("WKEND_SELNG_AMT") / col("WKEND_SELNG_CO")).alias("wkend_amt_per_co"),
        )

        df_two = df_data_two.orderBy(col("mdwk_amt_per_co").desc(), col("wkend_amt_per_co").desc())

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result1 = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])
        df_one.drop()
        df_two.drop()

        """
        방법 3
        남성_매출_금액 / 남성_매출_건수
        """
        df_data_three = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            (col("ML_SELNG_AMT") / col("ML_SELNG_CO")).alias("ml_amt_per_co"),
        )  # # 컬럼끼리 나눠 계산

        df_three = df_data_three.orderBy(col("ml_amt_per_co").desc())

        df_three = df_three.rdd.zipWithIndex().toDF()
        df_three = df_three.select(col("_1.*"), col("_2").alias("RANK_3"))
        df_three = df_three.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_3"),)

        """
        방법 3
        여성_매출_금액 / 여성_매출_건수
        """
        df_data_four = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            (col("FML_SELNG_AMT") / col("FML_SELNG_CO")).alias("fml_amt_per_co"),
        )  # # 컬럼끼리 나눠 계산

        df_four = df_data_four.orderBy(col("fml_amt_per_co").desc())

        df_four = df_four.rdd.zipWithIndex().toDF()
        df_four = df_four.select(col("_1.*"), col("_2").alias("RANK_4"))
        df_four = df_four.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_4"),)

        result2 = df_three.join(df_four, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])
        df_three.drop()
        df_four.drop()

        result = result1.join(result2, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])
        result1.drop()
        result2.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2") + col("RANK_3") + col("RANK_4"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )

        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK7"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_eight(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15573/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권배후지-추정매출)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            col("SVC_INDUTY_CD"),  #  col("서비스_업종_코드").alias("SVC_INDUTY_CD"),  # THSMON_SELNG_AMT,  THSMON_SELNG_CO
            # 비율
            col("MDWK_SELNG_RATE"),  # col("주중_매출_비율").alias("MDWK_SELNG_RATE"),
            col("WKEND_SELNG_RATE"),  # col("주말_매출_비율").alias("WKEND_SELNG_RATE"),
            # col("MON_SELNG_RATE"),  # col("월요일_매출_비율").alias("MON_SELNG_RATE"),
            # col("TUES_SELNG_RATE"),  # col("화요일_매출_비율").alias("TUES_SELNG_RATE"),
            # col("WED_SELNG_RATE"),  # col("수요일_매출_비율").alias("WED_SELNG_RATE"),
            # col("THUR_SELNG_RATE"),  # col("목요일_매출_비율").alias("THUR_SELNG_RATE"),
            # col("FRI_SELNG_RATE"),  # col("금요일_매출_비율").alias("FRI_SELNG_RATE"),
            # col("SAT_SELNG_RATE"),  # col("토요일_매출_비율").alias("SAT_SELNG_RATE"),
            # col("SUN_SELNG_RATE"),  # col("일요일_매출_비율").alias("SUN_SELNG_RATE"),
            col("ML_SELNG_RATE"),  # col("남성_매출_비율").alias("ML_SELNG_RATE"),
            col("FML_SELNG_RATE"),  # col("여성_매출_비율").alias("FML_SELNG_RATE"),
            # 금액
            col("MDWK_SELNG_AMT"),  # col("주중_매출_금액").alias("MDWK_SELNG_AMT"),
            col("WKEND_SELNG_AMT"),  # col("주말_매출_금액").alias("WKEND_SELNG_AMT"),
            # col("MON_SELNG_AMT"),  # col("월요일_매출_금액").alias("MON_SELNG_AMT"),
            # col("TUES_SELNG_AMT"),  # col("화요일_매출_금액").alias("TUES_SELNG_AMT"),
            # col("WED_SELNG_AMT"),  # col("수요일_매출_금액").alias("WED_SELNG_AMT"),
            # col("THUR_SELNG_AMT"),  # col("목요일_매출_금액").alias("THUR_SELNG_AMT"),
            # col("FRI_SELNG_AMT"),  # col("금요일_매출_금액").alias("FRI_SELNG_AMT"),
            # col("SAT_SELNG_AMT"),  # col("토요일_매출_금액").alias("SAT_SELNG_AMT"),
            # col("SUN_SELNG_AMT"),  # col("일요일_매출_금액").alias("SUN_SELNG_AMT"),
            col("ML_SELNG_AMT"),  # col("남성_매출_금액").alias("ML_SELNG_AMT"),
            col("FML_SELNG_AMT"),  # col("여성_매출_금액").alias("FML_SELNG_AMT"),
            # 건수
            col("MDWK_SELNG_CO"),  # col("주중_매출_건수").alias("MDWK_SELNG_CO"),
            col("WKEND_SELNG_CO"),  # col("주말_매출_건수").alias("WKEND_SELNG_CO"),
            # col("MON_SELNG_CO"),  # col("월요일_매출_건수").alias("MON_SELNG_CO"),
            # col("TUES_SELNG_CO"),  # col("화요일_매출_건수").alias("TUES_SELNG_CO"),
            # col("WED_SELNG_CO"),  # col("수요일_매출_건수").alias("WED_SELNG_CO"),
            # col("THUR_SELNG_CO"),  # col("목요일_매출_건수").alias("THUR_SELNG_CO"),
            # col("FRI_SELNG_CO"),  # col("금요일_매출_건수").alias("FRI_SELNG_CO"),
            # col("SAT_SELNG_CO"),  # col("토요일_매출_건수").alias("SAT_SELNG_CO"),
            # col("SUN_SELNG_CO"),  # col("일요일_매출_건수").alias("SUN_SELNG_CO"),
            col("ML_SELNG_CO"),  #  col("남성_매출_건수").alias("ML_SELNG_CO"),
            col("FML_SELNG_CO"),  # col("여성_매출_건수").alias("FML_SELNG_CO"),
        )

        return df_spark

    def calculate_eight(self, df_data: DataFrame, quarter: int = 1, service: str = "CS100001"):
        logging.error("calculate_eight start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.where((col("STDR_QU_CD") == quarter) & (col("SVC_INDUTY_CD") == service))
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        주중_매출_비율, 주말_매출_비율
        """

        df_data_one = df_data.select(
            col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("MDWK_SELNG_RATE"), col("WKEND_SELNG_RATE"),
        )  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        df_one = df_data_one.orderBy(col("MDWK_SELNG_RATE").desc(), col("WKEND_SELNG_RATE").desc(),)

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        주중_매출_금액 / 주중_매출_건수, 주말_매출_금액 / 주말_매출_건수
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            (col("MDWK_SELNG_AMT") / col("MDWK_SELNG_CO")).alias("mdwk_amt_per_co"),  # # 컬럼끼리 나눠 계산
            (col("WKEND_SELNG_AMT") / col("WKEND_SELNG_CO")).alias("wkend_amt_per_co"),
        )

        df_two = df_data_two.orderBy(col("mdwk_amt_per_co").desc(), col("wkend_amt_per_co").desc())

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result1 = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])
        df_one.drop()
        df_two.drop()

        """
        방법 3
        남성_매출_금액 / 남성_매출_건수
        """
        df_data_three = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            (col("ML_SELNG_AMT") / col("ML_SELNG_CO")).alias("ml_amt_per_co"),
        )  # # 컬럼끼리 나눠 계산

        df_three = df_data_three.orderBy(col("ml_amt_per_co").desc())

        df_three = df_three.rdd.zipWithIndex().toDF()
        df_three = df_three.select(col("_1.*"), col("_2").alias("RANK_3"))
        df_three = df_three.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_3"),)

        """
        방법 3
        여성_매출_금액 / 여성_매출_건수
        """
        df_data_four = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            (col("FML_SELNG_AMT") / col("FML_SELNG_CO")).alias("fml_amt_per_co"),
        )  # # 컬럼끼리 나눠 계산

        df_four = df_data_four.orderBy(col("fml_amt_per_co").desc())

        df_four = df_four.rdd.zipWithIndex().toDF()
        df_four = df_four.select(col("_1.*"), col("_2").alias("RANK_4"))
        df_four = df_four.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_4"),)

        result2 = df_three.join(df_four, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        result = result1.join(result2, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])
        result1.drop()
        result2.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2") + col("RANK_3") + col("RANK_4"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )

        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK8"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_nine(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15560/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권영역)
        """
        상권 코드랑 시군구 코드, 행정동 코드 변환
        """
        df_spark = df_spark.select(
            # 구분 코드
            col("상권_구분_코드").alias("TRDAR_SE_CD"),
            col("상권_구분_코드_명").alias("TRDAR_SE_CD_NM"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            col("시군구_코드").alias("SIGNGU_CD"),
            col("행정동_코드").alias("ADSTRD_CD"),
        )

        return df_spark

    def seoul_ten(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15566/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-아파트)

        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 아파트
            # col("APT_HSMP_CO"),  # col("아파트_단지_수").alias("APT_HSMP_CO"),
            col("AVRG_AE"),  # col("아파트_평균_면적").alias("AVRG_AE"),
            col("AVRG_MKTC"),  # col("아파트_평균_시가").alias("AVRG_MKTC"),
        )

        return df_spark

    def calculate_ten(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_ten start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None
        """
        방법 1
        아파트_단지_수, 아파트_평균_시가
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("AVRG_AE"), col("AVRG_MKTC"),
        )  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        df_one = df_data_one.orderBy(col("AVRG_MKTC").asc(), col("AVRG_AE").asc())

        result = df_one.rdd.zipWithIndex().toDF()
        df_one.drop()
        result = result.select(col("_1.*"), col("_2").alias("RANK10"))
        result = result.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK10"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_eleven(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15574/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권배후지-아파트)

        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 아파트
            # col("APT_HSMP_CO"),  # col("아파트_단지_수").alias("APT_HSMP_CO"),
            col("AVRG_AE"),  # col("아파트_평균_면적").alias("AVRG_AE"),
            col("AVRG_MKTC"),  # col("아파트_평균_시가").alias("AVRG_MKTC"),
        )

        return df_spark

    def calculate_eleven(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_eleven start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        아파트_단지_수, 아파트_평균_시가
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("AVRG_AE"), col("AVRG_MKTC"),
        )  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        df_one = df_data_one.orderBy(col("AVRG_MKTC").asc(), col("AVRG_AE").asc())

        result = df_one.rdd.zipWithIndex().toDF()
        df_one.drop()
        result = result.select(col("_1.*"), col("_2").alias("RANK11"))
        result = result.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK11"),)

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_fourteen(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15576/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-상권변화지표)

        # LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        # LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        # HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        # HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)

        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            # 상권 지표
            col("TRDAR_CHNGE_IX"),  # col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
            # col("OPR_SALE_MT_AVRG"),  # col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
            # col("CLS_SALE_MT_AVRG"),  # col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
            col("SU_OPR_SALE_MT_AVRG"),  # col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
            col("SU_CLS_SALE_MT_AVRG"),  # col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
        )

        return df_spark

    def calculate_fourteen(self, df_data: DataFrame, quarter: int = 1):
        logging.error("calculate_fourteen start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.filter(col("STDR_QU_CD") == quarter)
        if df_data.rdd.isEmpty():
            return None

        """
        방법 1
        상권_변화_지표, 서울_운영_영업_개월_평균, 서울_폐업_영업_개월_평균_-1
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("TRDAR_CHNGE_IX"),
            col("SU_OPR_SALE_MT_AVRG"),
            col("SU_CLS_SALE_MT_AVRG"),
        )  # 기준 년 코드  # 기준 분기 코드

        df_data_one = df_data_one.withColumn(
            "TRDAR_CHNGE_IX",
            when(col("TRDAR_CHNGE_IX") == "LL", 4)
            .when(col("TRDAR_CHNGE_IX") == "LH", 3)
            .when(col("TRDAR_CHNGE_IX") == "HL", 2)
            .otherwise(1),
        )

        df_data_one = df_data_one.orderBy(
            col("SU_OPR_SALE_MT_AVRG").asc(), col("TRDAR_CHNGE_IX").asc(), col("SU_CLS_SALE_MT_AVRG").desc(),
        )

        result = df_data_one.rdd.zipWithIndex().toDF()
        df_data_one.drop()
        result = result.select(col("_1.*"), col("_2").alias("RANK14"))
        result = result.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK14"),)

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )

        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def seoul_fifteen(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15577/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-점포)
        df_spark = df_spark.select(
            # 구분 코드
            col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
            col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
            col("TRDAR_CD"),  # col("상권_코드").alias("TRDAR_CD"),
            col("SVC_INDUTY_CD"),  # col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
            # 점표 지표
            col("STOR_CO"),  # col("점포_수").alias("STOR_CO"),
            # col("SIMILR_INDUTY_STOR_CO"),  # col("유사_업종_점포_수").alias("SIMILR_INDUTY_STOR_CO"),
            col("OPBIZ_RT"),  # col("개업_율").alias("OPBIZ_RT"),
            # col("OPBIZ_STOR_CO"),  # col("개업_점포_수").alias("OPBIZ_STOR_CO"),
            col("CLSBIZ_RT"),  # col("폐업_률").alias("CLSBIZ_RT"),
            # col("CLSBIZ_STOR_CO"),  # col("폐업_점포_수").alias("CLSBIZ_STOR_CO"),
            col("FRC_STOR_CO"),  # col("프랜차이즈_점포_수").alias("FRC_STOR_CO"),
        )

        return df_spark

    def calculate_fifteen(self, df_data: DataFrame, quarter: int = 1, service: str = "CS100001"):
        logging.error("calculate_fifteen start")
        df_data.createOrReplaceTempView("df")

        df_data = df_data.where((col("STDR_QU_CD") == quarter) & (col("SVC_INDUTY_CD") == service))
        if df_data.rdd.isEmpty():
            print("this is empty")
            return None

        """
        방법 1
        개업_율, 폐업_률
        """
        df_data_one = df_data.select(
            col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("OPBIZ_RT"), col("CLSBIZ_RT"),
        )  # 기준 년 코드  # 기준 분기 코드  # 상권_코드
        df_one = df_data_one.orderBy(col("CLSBIZ_RT").desc(), col("OPBIZ_RT").asc())

        df_one = df_one.rdd.zipWithIndex().toDF()
        df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
        df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

        """
        방법 2
        점포_수, 프랜차이즈_점포_수_1
        """
        df_data_two = df_data.select(
            col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("STOR_CO"), col("FRC_STOR_CO"),
        )  # 기준 년 코드  # 기준 분기 코드
        df_two = df_data_two.orderBy(col("STOR_CO").desc(), col("FRC_STOR_CO").desc())

        df_two = df_two.rdd.zipWithIndex().toDF()
        df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
        df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

        result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

        df_one.drop()
        df_two.drop()

        result = (
            result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
            .sort("RANK", ascending=False)
            .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
        )
        result = result.rdd.zipWithIndex().toDF()
        result = result.select(col("_1.*"), col("_2").alias("RANK15"))

        new_col_val = get_new_columns(result.rdd.map(lambda x: x.TRDAR_CD).collect())
        result = result.repartition(1).withColumn(
            "ADSTRD_CD", udf(lambda id: new_col_val[id])(monotonically_increasing_id())
        )
        # 설정 이후 drop
        self.spark.spark.catalog.dropTempView("df")
        return result

    def update_result_empty_col(self, df: DataFrame):
        for col in [
            "RANK1",
            "RANK2",
            "RANK3",
            "RANK4",
            "RANK5",
            "RANK6",
            "RANK7",
            "RANK8",
            "RANK10",
            "RANK11",
            "RANK14",
            "RANK15",
        ]:
            if col not in df.columns:
                print(f"col {col}")
                df = df.withColumn(colName=str(col), col=lit(0))

        return df

    def calc_final_result(self, df: DataFrame, year, quarter):
        final_report_df = df.withColumn(
            "TOTAL_SCORE",
            (
                col("RANK1")
                + col("RANK2")
                + col("RANK3")
                + col("RANK4")
                + col("RANK5")
                + col("RANK6")
                + col("RANK7")
                + col("RANK8")
                + col("RANK10")
                + col("RANK11")
                + col("RANK14")
                + col("RANK15")
            ).alias("TOTAL_SCORE"),
        )
        df.drop()
        final_report_df = final_report_df.orderBy(col("TOTAL_SCORE").desc())
        final_report_df = final_report_df.rdd.zipWithIndex().toDF()
        final_report_df = final_report_df.select(col("_1.*"), col("_2").alias("RANK"))

        # final_report_df.coalesce(1).write.option("header", "true").csv(
        #     f"s3a://rtc-result/spark/{year}_{quarter}_report_{int(time.time())}"
        # )
        return final_report_df
        save_df_result = final_report_df.toJSON().map(lambda x: json.loads(x)).collect()

        final_report_df.drop()
        return save_df_result

        # def seoul_twelve(self, df_spark: DataFrame) -> DataFrame:
        #     # https://data.seoul.go.kr/dataList/OA-15567/S/1/datasetView.do
        #     # 서울시 우리마을가게 상권분석서비스(자치구별 상권변화지표)

        #     # LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        #     # LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        #     # HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        #     # HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)

        #     df_spark = df_spark.select(
        #         # 구분 코드
        #         col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
        #         col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
        #         col("시군구_코드").alias("SIGNGU_CD"),
        #         # 상권 지표
        #         col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
        #         col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
        #         col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
        #         col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
        #         col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
        #     )

        #     return df_spark

        # def calculate_twelve(self, df_data: DataFrame, quarter : int = 1):
        #     logging.error("calculate_twelve start")
        #     df_data.createOrReplaceTempView("df")

        #     ve:
        #         """
        #         방법 1
        #         서울_운영_영업_개월_평균, 상권_변화_지표, 서울_폐업_영업_개월_평균_-1
        #         """
        #         df_data_one = df_data.select(
        #             col("STDR_YY_CD"),  # 기준 년 코드
        #             col("STDR_QU_CD"),  # 기준 분기 코드
        #             col("SIGNGU_CD"),
        #             col("TRDAR_CHNGE_IX"),
        #             col("SU_OPR_SALE_MT_AVRG"),
        #             col("SU_CLS_SALE_MT_AVRG"),
        #         )

        #         df_data_one = df_data_one.withColumn(
        #             "TRDAR_CHNGE_IX",
        #             when(col("TRDAR_CHNGE_IX") == "LL", 4)
        #             .when(col("TRDAR_CHNGE_IX") == "LH", 3)
        #             .when(col("TRDAR_CHNGE_IX") == "HL", 2)
        #             .otherwise(1),
        #         )

        #         df_data_one = df_data_one.orderBy(
        #             col("SU_OPR_SALE_MT_AVRG").asc(), col("TRDAR_CHNGE_IX").asc(), col("SU_CLS_SALE_MT_AVRG").desc(),
        #         )

        #         result = df_data_one.rdd.zipWithIndex().toDF()
        #         df_data_one.drop()
        #         result = result.select(col("_1.*"), col("_2").alias("RANK12"))
        #         result = result.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("SIGNGU_CD"), col("RANK12"),)

        #         result.show(10)

        #         # 설정 이후 drop
        #         self.spark.spark.catalog.dropTempView("df")

        #         self.res_twelve = result

        #     return self.res_twelve

        # def seoul_thirteen(self, df_spark: DataFrame) -> DataFrame:
        #     # https://data.seoul.go.kr/dataList/OA-15575/S/1/datasetView.do
        #     # 서울시 우리마을가게 상권분석서비스(행정동별 상권변화지표)

        #     # LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        #     # LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        #     # HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        #     # HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)

        #     df_spark = df_spark.select(
        #         # 구분 코드
        #         col("STDR_YY_CD"),  # col("기준_년_코드").alias("STDR_YY_CD"),
        #         col("STDR_QU_CD"),  # col("기준_분기_코드").alias("STDR_QU_CD"),
        #         col("행정동_코드").alias("ADSTRD_CD"),
        #         # 상권 지표
        #         col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
        #         col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
        #         col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
        #         col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
        #         col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
        #     )

        #     return df_spark

        # def calculate_thirteen(self, df_data: DataFrame, quarter : int = 1):
        #     logging.error("calculate_thirteen start")
        #     df_data.createOrReplaceTempView("df")

        #     teen:
        #         """
        #         방법 1
        #         상권_변화_지표, 서울_운영_영업_개월_평균, 서울_폐업_영업_개월_평균_-1
        #         """
        #         df_data_one = df_data.select(
        #             col("STDR_YY_CD"),  # 기준 년 코드
        #             col("STDR_QU_CD"),  # 기준 분기 코드
        #             col("ADSTRD_CD"),
        #             col("TRDAR_CHNGE_IX"),
        #             col("SU_OPR_SALE_MT_AVRG"),
        #             col("SU_CLS_SALE_MT_AVRG"),
        #         )

        #         df_data_one = df_data_one.withColumn(
        #             "TRDAR_CHNGE_IX",
        #             when(col("TRDAR_CHNGE_IX") == "LL", 4)
        #             .when(col("TRDAR_CHNGE_IX") == "LH", 3)
        #             .when(col("TRDAR_CHNGE_IX") == "HL", 2)
        #             .otherwise(1),
        #         )

        #         df_data_one = df_data_one.orderBy(
        #             col("SU_OPR_SALE_MT_AVRG").asc(), col("TRDAR_CHNGE_IX").asc(), col("SU_CLS_SALE_MT_AVRG").desc(),
        #         )

        #         result = df_data_one.rdd.zipWithIndex().toDF()
        #         df_data_one.drop()
        #         result = result.select(col("_1.*"), col("_2").alias("RANK13"))
        #         result = result.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("ADSTRD_CD"), col("RANK13"),)

        #         result.show(10)

        #         # 설정 이후 drop
        #         self.spark.spark.catalog.dropTempView("df")

        #         self.res_thirteen = result

        #     return self.res_thirteen

        # def seoul_sixteen(self, df_spark: DataFrame) -> DataFrame:
        #     # https://data.seoul.go.kr/dataList/OA-20471/S/1/datasetView.do
        #     # 서울시 불법주정차/전용차로 위반 단속 CCTV 위치정보

        #     df_spark = df_spark.select(
        #         # 구분 코드
        #         col("자치구").alias("PSTINST_CD"),
        #         col("위도").alias("LATITUDE"),
        #         col("경도").alias("LONGITUDE"),
        #     )

        #     return df_spark

        # def seoul_seventeen(self, df_spark:DataFrame) -> DataFrame:
        #     # https://data.seoul.go.kr/dataList/OA-13122/S/1/datasetView.do
        #     # 서울시 공영주차장 안내 정보

        #     df_spark = df_spark.select(
        #         # 구분 코드
        #         col("자치구").alias("PSTINST_CD"),
        #         col("위도").alias("LATITUDE"),
        #         col("경도").alias("LONGITUDE"),
        #     )

        #     return df_spark

        # def seoul_eighteen(self, df_spark:DataFrame) -> DataFrame:
        #     # http://data.seoul.go.kr/dataList/OA-15410/S/1/datasetView.do
        #     # 서울시 건축물대장 법정동 코드정보

        #     df_spark = df_spark.select(
        #         # 구분 코드
        #         col("자치구").alias("PSTINST_CD"),
        #         col("위도").alias("LATITUDE"),
        #         col("경도").alias("LONGITUDE"),
        #     )

    #     return df_spark

