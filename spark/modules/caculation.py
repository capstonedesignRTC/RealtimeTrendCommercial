import json
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from spark.spark_configure import SparkS3
from utils.utils import BUCKET_NAME, get_sys_args

from modules.dataframe import DF


class Calculate(object):
    def __init__(self, spark: SparkS3 = None, code_dict: dict = {}):
        self.code_dict = code_dict
        self.spark = spark
        self.df_func = DF()

    def reset_result_df(self):
        self.result.drop()
        del self.result

    def insert_specific_args(self, specific_args: list):
        self.specific_args = specific_args

    def make_pre_df(self, year, quarter) -> DataFrame:
        self.result = self.df_func.get_empty_dataframe()
        pre_data = []
        for val_1 in self.code_dict.values():
            for val_2 in val_1.values():
                for small in val_2.keys():
                    unknown_df = [int(year), int(quarter), int(small)]  # int(big), int(middle),
                    pre_data.append(unknown_df)

        rdd = self.df_func.spark.spark.sparkContext.parallelize(pre_data)
        unknown_df = self.df_func.spark.spark.createDataFrame(rdd, self.df_func.schema)
        self.result = self.result.unionByName(unknown_df)
        print("new self.result")

    def update_result_df(self, df: DataFrame):
        self.result = self.result.join(other=df, on=["STDR_YY_CD", "STDR_QU_CD", "TRDAR_CD"], how="fullouter").fillna(0)

    def update_result_df_with_zero(self, num: int):
        self.result = self.result.withColumn(f"RANK{num}", lit(0))

    def update_final_result_df(self, year: int = 2018, quarter: int = 1):
        final_report_df = self.result.select(
            col("STDR_YY_CD"),
            col("STDR_QU_CD"),
            col("TRDAR_CD"),
            col("RANK1"),
            col("RANK2"),
            col("RANK3"),
            col("RANK4"),
            col("RANK5"),
            col("RANK6"),
            col("RANK7"),
            col("RANK8"),
            col("RANK10"),
            col("RANK11"),
            col("RANK14"),
            col("RANK15"),
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

        self.result.drop()

        final_report_df = final_report_df.orderBy(col("TOTAL_SCORE").desc())
        final_report_df = final_report_df.rdd.zipWithIndex().toDF()
        final_report_df = final_report_df.select(col("_1.*"), col("_2").alias("RANK"))
        save_df_result = final_report_df.toJSON().map(lambda x: json.loads(x)).collect()

        self.spark.send_file(save_df_result, f"result/{year}_{quarter}_report.json")

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

        full_dict = {}
        for year in years:  # 해당 년도 가져오기
            if year not in full_dict:
                full_dict[year] = {}
            for quarter in quarters:
                self.make_pre_df(year, quarter)
                if quarter not in full_dict[year]:
                    full_dict[year][quarter] = {}

                for num in funcs:  # 해당 함수들을 가져오기
                    if num not in full_dict[year][quarter]:
                        full_dict[year][quarter][num] = None
                        try:
                            part_df_list = []
                            page = 1
                            while True:
                                file_name = f"{num}_{year}_{page}.csv"
                                df_spark = self.spark.get_file(file_name)
                                if df_spark is None:
                                    break
                                page += 1
                                part_df_list.append(df_spark)
                                return

                            full_df = part_df_list.pop()
                            while part_df_list:
                                part_df = part_df_list.pop()
                                full_df = full_df.union(part_df)

                            full_dict[year][quarter][num] = full_df

                            print("add dictionary")
                        except:
                            continue
                    try:
                        print("get from dictionary")
                        full_df = full_dict[year][quarter][num]
                        ###################

                        # # in local test
                        # page = 1
                        # file_name = f"{num}_{year}_{page}.csv"
                        # logging.error(f"try file name : {file_name}")
                        # full_df = self.spark.get_file(file_name)

                        if full_df is None:
                            self.update_result_df_with_zero(num)
                            logging.error(f"fail file name : {file_name}")
                            continue

                        logging.error(f"got file name : {file_name}")
                        try:
                            df_func, df_calc_func = self.df_func.get_function(num)
                            df_data = df_func(full_df)  # 우선은 분리시켜놓고 나중에 합치던가 하자
                            result_df = df_calc_func(df_data, quarter)
                            self.update_result_df(df=result_df)
                            logging.error(f"process success")

                        except Exception as e:
                            print(e.__str__())
                            self.update_result_df_with_zero(num)
                            continue

                        print("data send start")
                        save_df_result = result_df.toJSON().map(lambda x: json.loads(x)).collect()

                        self.spark.send_file(save_df_result, f"logs/{num}_{year}_{quarter}_report.json")

                        # result_df.rdd.coalesce(1).saveAsTextFile(f"s3://rtc-spark/result/{file_name}.csv")
                        # result_df.coalesce(1).write.option("header", "true").csv("s3a://rtc-spark/result_three.csv")
                        print("data send end")

                    except Exception as e:
                        print(e.__str__())
                        self.update_result_df_with_zero(num)

                    print("===================================")

                self.update_final_result_df(year, quarter)
                self.reset_result_df()

    # def calculation_all_cities(self):
    #     for big_code in self.code_dict.keys():  # 11545
    #         logging.error(f"this stage is {SEOUL_MUNICIPALITY_CODE[big_code]}")
    #         self.calculation_big_cities(big_code)

    # def calculation_big_cities(self, big_code: int):
    #     """
    #     시군구 코드(법정동 코드)로만 검색
    #     """
    #     for middle_code in self.code_dict[big_code].keys():
    #         logging.error(f"this stage is {middle_code}")
    #         self.calculation_middle_cities(big_code, middle_code)

    # def calculation_middle_cities(self, big_code: int, middle_code: int):
    #     """
    #     행정동 코드로만 검색
    #     """
    #     for small_code in self.code_dict[big_code][middle_code].keys():
    #         logging.error(f"this stage is {small_code}")
    #         self.calculation_small_cities(big_code, middle_code, small_code)

    # def calculation_small_cities(self, big: int, middle: int, small: int):
    #     """
    #     상권 코드로만 검색
    #     """
    #     years, quarters, funcs = get_sys_args(self.specific_args)

    #     year_df_list = []
    #     for year in years:  # 해당 년도 가져오기
    #         for num in funcs:  # 해당 함수들을 가져오기
    #             # part_df_list = []
    #             # page = 1
    #             # while True:
    #             #     file_name = f"{num}_{year}_{page}.csv"
    #             #     df_spark = self.spark.get_file(file_name)
    #             #     if df_spark is None:
    #             #         break
    #             #     page += 1
    #             #     part_df_list.append(df_spark)

    #             # full_df = part_df_list.pop()
    #             # while part_df_list:
    #             #     part_df = part_df_list.pop()
    #             #     full_df = full_df.union(part_df)

    #             # print(full_df.count())

    #             # full_df.show(1)

    #             ###################
    #             page = 1
    #             file_name = f"{num}_{year}_{page}.csv"
    #             logging.error(f"try file name : {file_name}")
    #             full_df = self.spark.get_file(file_name)

    #             if full_df is None:
    #                 logging.error(f"fail file name : {file_name}")
    #                 continue

    #             logging.error(f"got file name : {file_name}")
    #             df_func, df_calc_func = self.df_func.get_function(num)
    #             df_data = df_func(full_df)  # 우선은 분리시켜놓고 나중에 합치던가 하자
    #             result_df = df_calc_func(df_data)
    #             self.update_result_df(df=result_df)

    #         year_df_list.append(self.result)
    #         self.reset_result_df()
