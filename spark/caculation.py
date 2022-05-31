import logging

from pyspark.sql import DataFrame

from dataframe import DF
from spark.spark_configure import SparkS3
from utils.utils import SEOUL_MUNICIPALITY_CODE, get_sys_args


class Calculate(object):
    def __init__(self, spark: SparkS3 = None, code_dict: dict = {}):
        self.code_dict = code_dict
        self.spark = spark

        self.df_func = DF()
        self.result = self.df_func.get_empty_dataframe()

    def update_result(num: int, year: int, data: DataFrame):
        rank_idx = 2 * num + 3
        score_idx = 2 * num + 4
        # self.df_func 에 append로 udpateS

    def calculation_all_cities(self, specific_args: list):
        for big_code, middle_code_dict in self.code_dict.items():
            logging.info(f"this stage is {SEOUL_MUNICIPALITY_CODE[big_code]}")

    def calculation_big_cities(self, big: int, specific_args: list):
        """
        시군구 코드(법정동 코드)로만 검색
        """
        pass

    def calculation_middle_cities(self, big: int, middle: int, specific_args: list):
        """
        행정동 코드로만 검색
        """
        pass

    def calculation_small_cities(self, big: int, middle: int, small: int, specific_args: list):
        """
        상권 코드로만 검색
        """
        years, quarters, funcs = get_sys_args(specific_args)

        for num in funcs:  # 해당 함수들을 가져오기
            for year in years:  # 해당 년도 가져오기
                file_name = f"{num}_{year}.csv"  # 함수 이름
                print(file_name)
                # df_spark = self.spark.get_file(file_name) # 테스트 위해 파일 고정
                df_spark = self.spark.get_file_test()
                if df_spark is None:
                    continue

                df_func, df_calc_func = self.df_func.get_function(num)
                df_data = df_func(df_spark)  # 우선은 분리시켜놓고 나중에 합치던가 하자
                final_df_data = df_calc_func(df_data, year, big, middle, small)
                self.update_result(num=num, year=year, data=final_df_data)
                return
        pass

    def test(self, num) : 
        df_spark = self.spark.get_file_test()
        df_func, df_calc_func = self.df_func.get_function(num)
        df_data = df_func(df_spark)  # 우선은 분리시켜놓고 나중에 합치던가 하자
        final_df_data = df_calc_func(df_data, 2021, -1, -1, -1)
