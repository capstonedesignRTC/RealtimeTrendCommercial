import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, row_number, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window
from spark.spark_configure import SparkResult


def get_sk_code_to_hjd_code(df: DataFrame) -> dict():
    """
    상권 코드를 행정동 코드로 변환하는 data
    """
    df_spark = df.select(
        col("상권_코드").alias("sk_code"),
        col("시군구_코드").alias("sgk_code"),
        col("행정동_코드").alias("hjd_code"),
        col("상권_구분_코드").alias("sk_classification_code"),
    )
    df_spark.show(2)
    try:
        del df
    except:
        pass

    convert_dict = dict()
    for row in df_spark.rdd.collect():
        sk_code = row["sk_code"]
        sgk_code = row["sgk_code"]
        hjd_code = row["hjd_code"]
        sk_classification_code = row["sk_classification_code"]

        if sgk_code not in convert_dict:
            convert_dict[sgk_code] = dict()

        if hjd_code not in convert_dict[sgk_code]:  # 상권_구분_코드_명 저장
            convert_dict[sgk_code][hjd_code] = dict()

        if sk_code not in convert_dict[sgk_code][hjd_code]:
            convert_dict[sgk_code][hjd_code][sk_code] = sk_classification_code

    return convert_dict


class DF(object):
    schema = StructType(
        [
            StructField("year", IntegerType(), nullable=False),
            StructField("quarter", IntegerType(), nullable=False),
            StructField("big_code", IntegerType(), nullable=False),
            StructField("middle_code", IntegerType(), nullable=False),
            StructField("small_code", IntegerType(), nullable=False),
            # StructField("one_rank", IntegerType(), nullable=True),
            StructField("one_score", IntegerType(), nullable=True),
            # StructField("two_rank", IntegerType(), nullable=True),
            StructField("two_score", IntegerType(), nullable=True),
            # StructField("three_rank", IntegerType(), nullable=True),
            StructField("three_score", IntegerType(), nullable=True),
            # StructField("four_rank", IntegerType(), nullable=True),
            StructField("four_score", IntegerType(), nullable=True),
            # StructField("five_rank", IntegerType(), nullable=True),
            StructField("five_score", IntegerType(), nullable=True),
            # StructField("six_rank", IntegerType(), nullable=True),
            StructField("six_score", IntegerType(), nullable=True),
            # StructField("seven_rank", IntegerType(), nullable=True),
            StructField("seven_score", IntegerType(), nullable=True),
            # StructField("eight_rank", IntegerType(), nullable=True),
            StructField("eight_score", IntegerType(), nullable=True),
            # StructField("nine_rank", IntegerType(), nullable=True),
            StructField("nine_score", IntegerType(), nullable=True),
            # StructField("ten_rank", IntegerType(), nullable=True),
            StructField("ten_score", IntegerType(), nullable=True),
            # StructField("eleven_rank", IntegerType(), nullable=True),
            StructField("eleven_score", IntegerType(), nullable=True),
            # StructField("twelve_rank", IntegerType(), nullable=True),
            StructField("twelve_score", IntegerType(), nullable=True),
            # StructField("thirteen_rank", IntegerType(), nullable=True),
            StructField("thirteen_score", IntegerType(), nullable=True),
            # StructField("fourteen_rank", IntegerType(), nullable=True),
            StructField("fourteen_score", IntegerType(), nullable=True),
            # StructField("fifteen_rank", IntegerType(), nullable=True),
            StructField("fifteen_score", IntegerType(), nullable=True),
            # StructField("sixteen_rank", IntegerType(), nullable=True),
            StructField("sixteen_score", IntegerType(), nullable=True),
            StructField("total_score", IntegerType(), nullable=True),
            StructField("rank", IntegerType(), nullable=True),
        ]
    )

    def __init__(self):
        self.spark = SparkResult()

        self.res_one = dict()
        self.res_two = dict()
        self.res_three = dict()
        self.res_four = dict()
        self.res_five = dict()

    def get_empty_dataframe(self) -> DataFrame:

        df = self.spark.create_DF(self.schema)
        # df.printSchema()
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
        elif num == 9:
            return self.seoul_nine, self.calculate_nine
        elif num == 10:
            return self.seoul_ten, self.calculate_ten
        elif num == 11:
            return self.seoul_eleven, self.calculate_eleven
        elif num == 12:
            return self.seoul_twelve, self.calculate_twelve
        elif num == 13:
            return self.seoul_thirteen, self.calculate_thirteen
        elif num == 14:
            return self.seoul_fourteen, self.calculate_fourteen
        elif num == 15:
            return self.seoul_fifteen, self.calculate_fifteen
        elif num == 16:
            return self.seoul_sixteen, self.calculate_sixteen

    def seoul_one(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15568/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-생활인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("기준 년코드").alias("STDR_YY_CD"),
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

        df_spark.show(2)
        return df_spark

    def calculate_one(self, df_data: DataFrame, year: int):
        logging.error("calculate_one start")

        df_data.createOrReplaceTempView("df")

        if not self.res_one:
            """
            방법 1
            총 인구, 20대, 토요일, 일요일
            """
            df_data_one = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),  # 상권_코드
                col("TOT_FLPOP_CO"),
                col("AGRDE_20_FLPOP_CO"),
                col("SAT_FLPOP_CO"),
                col("SUN_FLPOP_CO"),
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
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),
                col("FML_FLPOP_CO"),
                col("AGRDE_30_FLPOP_CO"),
                col("AGRDE_40_FLPOP_CO"),
                col("TOT_FLPOP_CO"),
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
                .sort("RANK")
                .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
            )
            result = result.rdd.zipWithIndex().toDF()
            result = result.select(col("_1.*"), col("_2").alias("RANK1"))

            result.show(10)
            breakpoint()

            # 설정 이후 drop
            self.spark.spark.catalog.dropTempView("df")
            self.res_one = result

        return self.res_one, ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]

    def seoul_two(self, df_spark: DataFrame) -> DataFrame:
        # 서울시 우리마을가게 상권분석서비스(상권-직장인구)
        # https://data.seoul.go.kr/dataList/OA-15569/S/1/datasetView.do
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년월_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            # 인구수
            col("총_직장_인구_수").alias("TOT_WRC_POPLTN_CO"),
            col("남성_직장_인구_수").alias("ML_WRC_POPLTN_CO"),
            col("여성_직장_인구_수").alias("FML_WRC_POPLTN_CO"),
            # 연령대
            col("연령대_10_직장_인구_수").alias("AGRDE_10_WRC_POPLTN_CO"),
            col("연령대_20_직장_인구_수").alias("AGRDE_20_WRC_POPLTN_CO"),
            col("연령대_30_직장_인구_수").alias("AGRDE_30_WRC_POPLTN_CO"),
            col("연령대_40_직장_인구_수").alias("AGRDE_40_WRC_POPLTN_CO"),
            col("연령대_50_직장_인구_수").alias("AGRDE_50_WRC_POPLTN_CO"),
            col("연령대_60_이상_직장_인구_수").alias("AGRDE_60_ABOVE_WRC_POPLTN_CO"),
        )

        df_spark.show(2)
        return df_spark

    def calculate_two(self, df_data: DataFrame, year: int):
        logging.error("calculate_two start")

        df_data.createOrReplaceTempView("df")

        if not self.res_two:
            """
            방법 1
            총_직장_인구_수, 연령대_30_직장_인구_수, 연령대_40_직장_인구_수
            """
            df_data_one = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),  # 상권_코드
                col("TOT_WRC_POPLTN_CO"),
                col("AGRDE_30_WRC_POPLTN_CO"),
                col("AGRDE_40_WRC_POPLTN_CO"),
            )
            df_one = df_data_one.orderBy(
                col("TOT_WRC_POPLTN_CO").desc(),
                col("AGRDE_30_WRC_POPLTN_CO").desc(),
                col("AGRDE_40_WRC_POPLTN_CO").desc(),
            )

            df_one = df_one.rdd.zipWithIndex().toDF()
            df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
            df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

            """
            방법 2
            남성_직장_인구_수, 연령대_40_직장_인구_수, 연령대_50_직장_인구_수
            """
            df_data_two = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),
                col("ML_WRC_POPLTN_CO"),
                col("AGRDE_40_WRC_POPLTN_CO"),
                col("AGRDE_50_WRC_POPLTN_CO"),
            )
            df_two = df_data_two.orderBy(
                col("ML_WRC_POPLTN_CO").desc(),
                col("AGRDE_40_WRC_POPLTN_CO").desc(),
                col("AGRDE_50_WRC_POPLTN_CO").desc(),
            )

            df_two = df_two.rdd.zipWithIndex().toDF()
            df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
            df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

            result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

            df_one.drop()
            df_two.drop()

            result = (
                result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
                .sort("RANK")
                .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
            )
            result = result.rdd.zipWithIndex().toDF()
            result = result.select(col("_1.*"), col("_2").alias("RANK2"))

            result.show(10)

            # 설정 이후 drop
            self.spark.spark.catalog.dropTempView("df")
            self.res_one = result

        return self.res_one, ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]

    def seoul_three(self, df_spark: DataFrame) -> DataFrame:
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

    def calculate_three(self, df_data: DataFrame, year: int):  # , big: int, middle: int, small: int):
        logging.error("calculate_three start")
        df_data.createOrReplaceTempView("df")

        if not self.res_three:
            """
            방법 1
            총 인구, 20대, 토요일, 일요일
            """
            df_data_one = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),
                col("TOT_FLPOP_CO"),
                col("AGRDE_20_FLPOP_CO"),
                col("SAT_FLPOP_CO"),
                col("SUN_FLPOP_CO"),
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
            df_one.show(3)
            """
            +----------+----------+--------+------+
            |STDR_YY_CD|STDR_QU_CD|TRDAR_CD|RANK_1|
            +----------+----------+--------+------+
            |      2021|         4| 2110948|     0|
            |      2021|         3| 2110948|     1|
            |      2021|         2| 2110948|     2|
            +----------+----------+--------+------+      
                        
            """

            """
            방법 2
            여성 인구, 30대, 40대, 총_생활인구_수
            """
            df_data_two = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),
                col("FML_FLPOP_CO"),
                col("AGRDE_30_FLPOP_CO"),
                col("AGRDE_40_FLPOP_CO"),
                col("TOT_FLPOP_CO"),
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
            # one_two.show(3)
            """
            +----------+----------+--------+------+
            |STDR_YY_CD|STDR_QU_CD|TRDAR_CD|RANK_2|
            +----------+----------+--------+------+
            |      2021|         4| 2110948|     0|
            |      2021|         3| 2110948|     1|
            |      2021|         2| 2110948|     2|
            +----------+----------+--------+------+
            """

            result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

            df_one.drop()
            df_two.drop()

            result = (
                result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
                .sort("RANK")
                .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
            )
            result = result.rdd.zipWithIndex().toDF()
            result = result.select(col("_1.*"), col("_2").alias("RANK3"))

            result.show(10)
            """
            +----------+----------+--------+----+
            |STDR_YY_CD|STDR_QU_CD|TRDAR_CD|RANK|
            +----------+----------+--------+----+
            |      2021|         4| 2110532|   0|
            |      2021|         4| 2110533|   1|
            |      2021|         2| 2110532|   2|
            |      2021|         2| 2110533|   3|
            |      2021|         3| 2110532|   4|
            |      2021|         1| 2110532|   5|
            |      2021|         3| 2110533|   6|
            |      2021|         1| 2110533|   7|
            |      2021|         1| 2110656|   8|
            |      2021|         1| 2110654|   9|
            +----------+----------+--------+----+

            """
            # 설정 이후 drop
            self.spark.spark.catalog.dropTempView("df")
            self.res_three = result

        return self.res_three, ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]

    def seoul_four(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15570/S/1/datasetView.do

        # 서울시 우리마을가게 상권분석서비스(상권배후지-직장인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            # 인구수
            col("총_직장_인구_수").alias("TOT_WRC_POPLTN_CO"),
            col("남성_직장_인구_수").alias("ML_WRC_POPLTN_CO"),
            col("여성_직장_인구_수").alias("FML_WRC_POPLTN_CO"),
            # 연령대
            col("연령대_10_직장_인구_수").alias("AGRDE_10_WRC_POPLTN_CO"),
            col("연령대_20_직장_인구_수").alias("AGRDE_20_WRC_POPLTN_CO"),
            col("연령대_30_직장_인구_수").alias("AGRDE_30_WRC_POPLTN_CO"),
            col("연령대_40_직장_인구_수").alias("AGRDE_40_WRC_POPLTN_CO"),
            col("연령대_50_직장_인구_수").alias("AGRDE_50_WRC_POPLTN_CO"),
            col("연령대_60_이상_직장_인구_수").alias("AGRDE_60_ABOVE_WRC_POPLTN_CO"),
        )

        df_spark.show(2)
        return df_spark

    def calculate_four(self, df_data: DataFrame, year: int):
        logging.error("calculate_two start")
        df_data.createOrReplaceTempView("df")

        if not self.res_four:
            """
            방법 1
            총_직장_인구_수, 연령대_30_직장_인구_수, 연령대_40_직장_인구_수
            """
            df_data_one = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),  # 상권_코드
                col("TOT_WRC_POPLTN_CO"),
                col("AGRDE_30_WRC_POPLTN_CO"),
                col("AGRDE_40_WRC_POPLTN_CO"),
            )
            df_one = df_data_one.orderBy(
                col("TOT_WRC_POPLTN_CO").desc(),
                col("AGRDE_30_WRC_POPLTN_CO").desc(),
                col("AGRDE_40_WRC_POPLTN_CO").desc(),
            )

            df_one = df_one.rdd.zipWithIndex().toDF()
            df_one = df_one.select(col("_1.*"), col("_2").alias("RANK_1"))
            df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_1"),)

            """
            방법 2
            남성_직장_인구_수, 연령대_40_직장_인구_수, 연령대_50_직장_인구_수
            """
            df_data_two = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),
                col("ML_WRC_POPLTN_CO"),
                col("AGRDE_40_WRC_POPLTN_CO"),
                col("AGRDE_50_WRC_POPLTN_CO"),
            )
            df_two = df_data_two.orderBy(
                col("ML_WRC_POPLTN_CO").desc(),
                col("AGRDE_40_WRC_POPLTN_CO").desc(),
                col("AGRDE_50_WRC_POPLTN_CO").desc(),
            )

            df_two = df_two.rdd.zipWithIndex().toDF()
            df_two = df_two.select(col("_1.*"), col("_2").alias("RANK_2"))
            df_two = df_two.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK_2"),)

            result = df_one.join(df_two, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"])

            df_one.drop()
            df_two.drop()

            result = (
                result.withColumn("RANK", col("RANK_1") + col("RANK_2"))
                .sort("RANK")
                .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
            )
            result = result.rdd.zipWithIndex().toDF()
            result = result.select(col("_1.*"), col("_2").alias("RANK4"))

            result.show(10)

            # 설정 이후 drop
            self.spark.spark.catalog.dropTempView("df")
            self.res_one = result

        return self.res_one, ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]

    def seoul_five(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15584/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권_상주인구)
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권 코드").alias("TRDAR_CD"),
            # 인구수
            col("총 상주인구 수").alias("TOT_REPOP_CO"),
            col("남성 상주인구 수").alias("ML_REPOP_CO"),
            col("여성 상주인구 수").alias("FML_REPOP_CO"),
            # 연령대
            col("연령대 10 상주인구 수").alias("AGRDE_10_REPOP_CO"),
            col("연령대 20 상주인구 수").alias("AGRDE_20_REPOP_CO"),
            col("연령대 30 상주인구 수").alias("AGRDE_30_REPOP_CO"),
            col("연령대 40 상주인구 수").alias("AGRDE_40_REPOP_CO"),
            col("연령대 50 상주인구 수").alias("AGRDE_50_REPOP_CO"),
            col("연령대 60 이상 상주인구 수").alias("AGRDE_60_ABOVE_REPOP_CO"),
            # 총 가구 수
            col("총 가구 수").alias("TOT_HSHLD_CO"),
            col("아파트 가구 수").alias("APT_HSHLD_CO"),
            col("비 아파트 가구 수").alias("NON_APT_HSHLD_CO"),
        )

        df_spark.show(2)
        return df_spark

    def calculate_five(self, df_data: DataFrame, year: int):
        logging.error("calculate_five start")
        df_data.createOrReplaceTempView("df")

        if not self.res_two:
            """
            방법 1
            총 상주인구 수, 연령대 30 상주인구 수, 연령대 40 상주인구 수
            """
            df_data_one = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),  # 상권_코드
                col("TOT_REPOP_CO"),
                col("AGRDE_30_REPOP_CO"),
                col("AGRDE_40_REPOP_CO"),
            )
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
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),
                col("TOT_HSHLD_CO"),
                col("APT_HSHLD_CO"),
                col("NON_APT_HSHLD_CO"),
            )
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
                .sort("RANK")
                .select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"))
            )
            result = result.rdd.zipWithIndex().toDF()
            result = result.select(col("_1.*"), col("_2").alias("RANK5"))

            result.show(10)

            # 설정 이후 drop
            self.spark.spark.catalog.dropTempView("df")
            self.res_one = result

        return self.res_one, ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]

    def seoul_six(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-21278/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-소득소비)
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            # 금액
            col("월_평균_소득_금액").alias("MT_AVRG_INCOME_AMT"),
            col("소득_구간_코드").alias("INCOME_SCTN_CD"),
            col("지출_총금액").alias("EXPNDTR_TOTAMT"),
            col("식료품_지출_총금액").alias("FDSTFFS_EXPNDTR_TOTAMT"),
            col("의류_신발_지출_총금액").alias("CLTHS_FTWR_EXPNDTR_TOTAMT"),
            col("생활용품_지출_총금액").alias("LVSPL_EXPNDTR_TOTAMT"),
            col("의료비_지출_총금액").alias("MCP_EXPNDTR_TOTAMT"),
            col("교통_지출_총금액").alias("TRNSPORT_EXPNDTR_TOTAMT"),
            col("여가_지출_총금액").alias("LSR_EXPNDTR_TOTAMT"),
            col("문화_지출_총금액").alias("CLTUR_EXPNDTR_TOTAMT"),
            col("교육_지출_총금액").alias("EDC_EXPNDTR_TOTAMT"),
            col("유흥_지출_총금액").alias("PLESR_EXPNDTR_TOTAMT"),
        )

        df_spark.show(2)
        return df_spark

    def calculate_six(self, df_data: DataFrame, year: int):
        logging.error("calculate_six start")
        df_data.createOrReplaceTempView("df")

        if not self.res_two:
            """
            방법 1
            월_평균_소득_금액, 지출_총금액, 소득_구간_코드
            """
            df_data_one = df_data.select(
                col("STDR_YY_CD"),  # 기준 년 코드
                col("STDR_QU_CD"),  # 기준 분기 코드
                col("TRDAR_CD"),  # 상권_코드
                col("MT_AVRG_INCOME_AMT"),
                col("EXPNDTR_TOTAMT"),
                col("INCOME_SCTN_CD"),
            )
            df_one = df_data_one.orderBy(
                col("MT_AVRG_INCOME_AMT").desc(), col("EXPNDTR_TOTAMT").desc(), col("INCOME_SCTN_CD").desc(),
            )

            df_one = df_one.rdd.zipWithIndex().toDF()
            df_one = df_one.select(col("_1.*"), col("_2").alias("RANK6"))
            df_one = df_one.select(col("STDR_YY_CD"), col("STDR_QU_CD"), col("TRDAR_CD"), col("RANK6"),)

            """
            방법 2
            지출 비용 순위 매기기
            """

            df_one.show(10)

            # 설정 이후 drop
            self.spark.spark.catalog.dropTempView("df")
            self.res_one = df_one

        return self.res_one, ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]

    def seoul_seven(self, df_spark: DataFrame) -> DataFrame:
        # http://data.seoul.go.kr/dataList/OA-15572/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-추정매출)
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
            # 비율
            col("주중_매출_비율").alias("MDWK_SELNG_RATE"),
            col("주말_매출_비율").alias("WKEND_SELNG_RATE"),
            col("월요일_매출_비율").alias("MON_SELNG_RATE"),
            col("화요일_매출_비율").alias("TUES_SELNG_RATE"),
            col("수요일_매출_비율").alias("WED_SELNG_RATE"),
            col("목요일_매출_비율").alias("THUR_SELNG_RATE"),
            col("금요일_매출_비율").alias("FRI_SELNG_RATE"),
            col("토요일_매출_비율").alias("SAT_SELNG_RATE"),
            col("일요일_매출_비율").alias("SUN_SELNG_RATE"),
            col("남성_매출_비율").alias("ML_SELNG_RATE"),
            col("여성_매출_비율").alias("FML_SELNG_RATE"),
            # 금액
            col("주중_매출_금액").alias("MDWK_SELNG_AMT"),
            col("주말_매출_금액").alias("WKEND_SELNG_AMT"),
            col("월요일_매출_금액").alias("MON_SELNG_AMT"),
            col("화요일_매출_금액").alias("TUES_SELNG_AMT"),
            col("수요일_매출_금액").alias("WED_SELNG_AMT"),
            col("목요일_매출_금액").alias("THUR_SELNG_AMT"),
            col("금요일_매출_금액").alias("FRI_SELNG_AMT"),
            col("토요일_매출_금액").alias("SAT_SELNG_AMT"),
            col("일요일_매출_금액").alias("SUN_SELNG_AMT"),
            col("남성_매출_금액").alias("ML_SELNG_AMT"),
            col("여성_매출_금액").alias("FML_SELNG_AMT"),
            # 건수
            col("주중_매출_건수").alias("MDWK_SELNG_CO"),
            col("주말_매출_건수").alias("WKEND_SELNG_CO"),
            col("월요일_매출_건수").alias("MON_SELNG_CO"),
            col("화요일_매출_건수").alias("TUES_SELNG_CO"),
            col("수요일_매출_건수").alias("WED_SELNG_CO"),
            col("목요일_매출_건수").alias("THUR_SELNG_CO"),
            col("금요일_매출_건수").alias("FRI_SELNG_CO"),
            col("토요일_매출_건수").alias("SAT_SELNG_CO"),
            col("일요일_매출_건수").alias("SUN_SELNG_CO"),
            col("남성_매출_건수").alias("ML_SELNG_CO"),
            col("여성_매출_건수").alias("FML_SELNG_CO"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_eight(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15573/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권배후지-추정매출)
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
            # 비율
            col("주중_매출_비율").alias("MDWK_SELNG_RATE"),
            col("주말_매출_비율").alias("WKEND_SELNG_RATE"),
            col("월요일_매출_비율").alias("MON_SELNG_RATE"),
            col("화요일_매출_비율").alias("TUES_SELNG_RATE"),
            col("수요일_매출_비율").alias("WED_SELNG_RATE"),
            col("목요일_매출_비율").alias("THUR_SELNG_RATE"),
            col("금요일_매출_비율").alias("FRI_SELNG_RATE"),
            col("토요일_매출_비율").alias("SAT_SELNG_RATE"),
            col("일요일_매출_비율").alias("SUN_SELNG_RATE"),
            col("남성_매출_비율").alias("ML_SELNG_RATE"),
            col("여성_매출_비율").alias("FML_SELNG_RATE"),
            # 금액
            col("주중_매출_금액").alias("MDWK_SELNG_AMT"),
            col("주말_매출_금액").alias("WKEND_SELNG_AMT"),
            col("월요일_매출_금액").alias("MON_SELNG_AMT"),
            col("화요일_매출_금액").alias("TUES_SELNG_AMT"),
            col("수요일_매출_금액").alias("WED_SELNG_AMT"),
            col("목요일_매출_금액").alias("THUR_SELNG_AMT"),
            col("금요일_매출_금액").alias("FRI_SELNG_AMT"),
            col("토요일_매출_금액").alias("SAT_SELNG_AMT"),
            col("일요일_매출_금액").alias("SUN_SELNG_AMT"),
            col("남성_매출_금액").alias("ML_SELNG_AMT"),
            col("여성_매출_금액").alias("FML_SELNG_AMT"),
            # 건수
            col("주중_매출_건수").alias("MDWK_SELNG_CO"),
            col("주말_매출_건수").alias("WKEND_SELNG_CO"),
            col("월요일_매출_건수").alias("MON_SELNG_CO"),
            col("화요일_매출_건수").alias("TUES_SELNG_CO"),
            col("수요일_매출_건수").alias("WED_SELNG_CO"),
            col("목요일_매출_건수").alias("THUR_SELNG_CO"),
            col("금요일_매출_건수").alias("FRI_SELNG_CO"),
            col("토요일_매출_건수").alias("SAT_SELNG_CO"),
            col("일요일_매출_건수").alias("SUN_SELNG_CO"),
            col("남성_매출_건수").alias("ML_SELNG_CO"),
            col("여성_매출_건수").alias("FML_SELNG_CO"),
        )

        df_spark.show(2)
        return df_spark

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
            col("상권_코드").alias("TRDAR_CD"),
            col("시군구_코드").alias("SIGNGU_CD"),
            col("행정동_코드").alias("ADSTRD_CD"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_ten(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15566/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-아파트)

        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            # 아파트
            col("아파트_단지_수").alias("APT_HSMP_CO"),
            col("아파트_평균_면적").alias("AVRG_AE"),
            col("아파트_평균_시가").alias("AVRG_MKTC"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_eleven(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15574/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권배후지-아파트)

        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            # 아파트
            col("아파트_단지_수").alias("APT_HSMP_CO"),
            col("아파트_평균_면적").alias("AVRG_AE"),
            col("아파트_평균_시가").alias("AVRG_MKTC"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_twelve(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15567/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(자치구별 상권변화지표)

        """
        LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        """
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("시군구_코드").alias("SIGNGU_CD"),
            # 상권 지표
            col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
            col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
            col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
            col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
            col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_thirteen(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15575/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(행정동별 상권변화지표)
        """
        LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        """
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("행정동_코드").alias("ADSTRD_CD"),
            # 상권 지표
            col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
            col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
            col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
            col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
            col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_fourteen(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15576/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-상권변화지표)
        """
        LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
        HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
        """
        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            # 상권 지표
            col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
            col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
            col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
            col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
            col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_fifteen(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-15577/S/1/datasetView.do
        # 서울시 우리마을가게 상권분석서비스(상권-점포)

        df_spark = df_spark.select(
            # 구분 코드
            col("기준_년_코드").alias("STDR_YY_CD"),
            col("기준_분기_코드").alias("STDR_QU_CD"),
            col("상권_코드").alias("TRDAR_CD"),
            col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
            # 점표 지표
            col("점포_수").alias("STOR_CO"),
            col("유사_업종_점포_수").alias("SIMILR_INDUTY_STOR_CO"),
            col("개업_율").alias("OPBIZ_RT"),
            col("개업_점포_수").alias("OPBIZ_STOR_CO"),
            col("폐업_률").alias("CLSBIZ_RT"),
            col("폐업_점포_수").alias("CLSBIZ_STOR_CO"),
            col("프랜차이즈_점포_수").alias("FRC_STOR_CO"),
        )

        df_spark.show(2)
        return df_spark

    def seoul_sixteen(self, df_spark: DataFrame) -> DataFrame:
        # https://data.seoul.go.kr/dataList/OA-20471/S/1/datasetView.do
        # 서울시 불법주정차/전용차로 위반 단속 CCTV 위치정보

        df_spark = df_spark.select(
            # 구분 코드
            col("자치구").alias("PSTINST_CD"),
            col("위도").alias("LATITUDE"),
            col("경도").alias("LONGITUDE"),
        )

        df_spark.show(2)
        return df_spark

        # def seoul_seventeen(self, df_spark:DataFrame) -> DataFrame:
        #     # https://data.seoul.go.kr/dataList/OA-13122/S/1/datasetView.do
        #     # 서울시 공영주차장 안내 정보

        #     df_spark = df_spark.select(
        #         # 구분 코드
        #         col("자치구").alias("PSTINST_CD"),
        #         col("위도").alias("LATITUDE"),
        #         col("경도").alias("LONGITUDE"),
        #     )

        #     df_spark.show(2)
        #     return df_spark

        def seoul_seventeen(self, df_spark: DataFrame) -> DataFrame:
            # http://data.seoul.go.kr/dataList/OA-15410/S/1/datasetView.do
            # 서울시 건축물대장 법정동 코드정보

            df_spark = df_spark.select(
                # 구분 코드
                col("자치구").alias("PSTINST_CD"),
                col("위도").alias("LATITUDE"),
                col("경도").alias("LONGITUDE"),
            )

            df_spark.show(2)

        return df_spark

    # def seoul_eighteen(self, df_spark:DataFrame) -> DataFrame:
    #     # http://data.seoul.go.kr/dataList/OA-15410/S/1/datasetView.do
    #     # 서울시 건축물대장 법정동 코드정보

    #     df_spark = df_spark.select(
    #         # 구분 코드
    #         col("자치구").alias("PSTINST_CD"),
    #         col("위도").alias("LATITUDE"),
    #         col("경도").alias("LONGITUDE"),
    #     )

    #     df_spark.show(2)
    #     return df_spark
