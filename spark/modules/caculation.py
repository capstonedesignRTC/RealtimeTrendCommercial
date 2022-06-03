import json
import logging

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from spark.spark_configure import SparkS3
from my_secret import profile_info
from utils.utils import SEOUL_MUNICIPALITY_CODE, get_sys_args

from modules.dataframe import DF


class Calculate(object):
    def __init__(self, spark: SparkS3 = None, code_dict: dict = {}):
        self.code_dict = code_dict
        self.spark = spark

        self.df_func = DF()
        self.result = self.df_func.get_empty_dataframe()

    def insert_specific_args(self, specific_args: list):
        self.specific_args = specific_args

    def make_pre_df(self, year, quarter):
        pre_data = []
        for big, val_1 in self.code_dict.items():
            for middle, val_2 in val_1.items():
                for small in val_2.keys():
                    unknown_df = [int(year), int(quarter), int(big), int(middle), int(small)]

                    while len(unknown_df) != 23:
                        unknown_df.append(0)

                    pre_data.append(unknown_df)

        rdd = self.df_func.spark.spark.sparkContext.parallelize(pre_data)  # SparkContext.parallelize(pre_data)
        unknown_df = self.df_func.spark.spark.createDataFrame(rdd, self.df_func.schema)
        self.result = self.result.unionByName(unknown_df)

    def update_result(self, num: int, year: int, data: DataFrame, big: int, middle: int, small: int):
        pass

    def update_result_df(self, df, on: list = ["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"]):
        self.result = self.result.join(df, on=on).sort("RANK3")
        breakpoint()
        self.result.show()

    def calculation_all_cities(self):
        for big_code in self.code_dict.keys():  # 11545
            logging.error(f"this stage is {SEOUL_MUNICIPALITY_CODE[big_code]}")
            self.calculation_big_cities(big_code)

    def calculation_big_cities(self, big_code: int):
        """
        시군구 코드(법정동 코드)로만 검색
        """
        for middle_code in self.code_dict[big_code].keys():
            logging.error(f"this stage is {middle_code}")
            self.calculation_middle_cities(big_code, middle_code)

    def calculation_middle_cities(self, big_code: int, middle_code: int):
        """
        행정동 코드로만 검색
        """
        for small_code in self.code_dict[big_code][middle_code].keys():
            logging.error(f"this stage is {small_code}")
            self.calculation_small_cities(big_code, middle_code, small_code)

    def calculation_small_cities(self, big: int, middle: int, small: int):
        """
        상권 코드로만 검색
        """

        years, quarters, funcs = get_sys_args(self.specific_args)
        print(years, quarters, funcs)

        for num in funcs:  # 해당 함수들을 가져오기
            for year in years:  # 해당 년도 가져오기
                file_name = f"{num}_{year}.csv"  # 함수 이름

                file_name = "test_four.csv"
                print(file_name)
                df_spark = self.spark.get_file(file_name)  # 테스트 위해 파일 고정
                # df_spark = self.spark.get_file_test()
                if df_spark is None:
                    continue
                num = 4
                df_func, df_calc_func = self.df_func.get_function(num)
                df_data = df_func(df_spark)  # 우선은 분리시켜놓고 나중에 합치던가 하자
                final_df_val = df_calc_func(df_data, year, big, middle, small)  # value
                self.update_result(num, year, final_df_val, big, middle, small)
                return
        pass

    def find_city_code(self, code, is_middle=True):
        for big, val1 in self.code_dict.items():
            for middle, val2 in val1.items():
                if middle == code:
                    return big
                if is_middle:
                    continue
                for small in val2.keys():
                    if small == code:
                        return big, middle

    def calculation_all_founds(self):

        years, quarters, funcs = get_sys_args(self.specific_args)
        print(years, quarters, funcs)

        for year in years:  # 해당 년도 가져오기
            # self.make_pre_df(year, 0)
            for num in funcs:  # 해당 함수들을 가져오기
                file_name = f"{num}_{year}.csv"  # 함수 이름

                num = 7
                file_name = "test_seven.csv"
                print(file_name)
                df_spark = self.spark.get_file(file_name)  # 테스트 위해 파일 고정
                # df_spark = self.spark.get_file_test()
                if df_spark is None:
                    continue

                df_func, df_calc_func = self.df_func.get_function(num)
                df_data = df_func(df_spark)  # 우선은 분리시켜놓고 나중에 합치던가 하자
                result_df, on_list = df_calc_func(df_data, year)  # value

                return
                # res = result_df.rdd.coalesce(1).saveAsTextFile("s3://rtc-spark/result_three.csv")
                print("data send start")

                save_df_result = result_df.toJSON().map(lambda x: json.loads(x)).collect()
                self.spark.s3_client.put_object(
                    Body=json.dumps(save_df_result), Bucket=profile_info["aws_bucket_name"], Key="result_three.json"
                )
                # result_df.coalesce(1).write.option("header", "true").csv("s3a://rtc-spark/result_three.csv")

                print("data send end")
                # self.update_result(num, year, result_df)

                self.update_result_df(df=result_df, on=on_list)
                return
        pass
