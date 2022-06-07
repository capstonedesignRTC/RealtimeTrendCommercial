import json
import logging
import time

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from spark.configure import SparkS3

from modules.dataframe import DF
from modules.utils import CODE, get_sys_args


class Calculate(object):
    def __init__(self, spark: SparkS3 = None):
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

        for val_1 in CODE.values():
            for middle in val_1.keys():
                unknown_df = [int(year), int(quarter), str(middle)]
                pre_data.append(unknown_df)

        rdd = self.df_func.spark.spark.sparkContext.parallelize(pre_data)
        unknown_df = self.df_func.spark.spark.createDataFrame(rdd, self.df_func.schema)
        self.result = self.result.unionByName(unknown_df)
        print("create new dataframe")

    def update_result_df(self, df: DataFrame):
        self.result = self.result.join(other=df, on=["STDR_YY_CD", "STDR_QU_CD", "ADSTRD_CD"], how="fullouter").fillna(
            0
        )

    def update_result_df_with_zero(self, num: int):
        self.result = self.result.withColumn(f"RANK{num}", lit(0))

    def update_final_result_df(self, year: int = 2018, quarter: int = 1):
        result = self.df_func.update_result_empty_col(self.result)
        result = self.df_func.calc_final_result(result)
        self.spark.send_file(result, f"result/new/{year}_{quarter}_report.json")

    def calculation_all_founds(self):
        years, quarters, funcs = get_sys_args(self.specific_args)

        full_dict = {}
        for year in years:
            if year not in full_dict:
                full_dict[year] = {}
            for quarter in quarters:
                self.make_pre_df(year, quarter)
                if quarter not in full_dict[year]:
                    full_dict[year][quarter] = {}

                for num in funcs:
                    if num not in full_dict[year][quarter]:
                        full_dict[year][quarter][num] = None

                        if num in [3, 7, 8, 15]:
                            self.update_result_df_with_zero(num)
                            continue
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
                                if page == 100:
                                    break

                            full_df = part_df_list.pop()
                            while part_df_list:
                                part_df = part_df_list.pop()
                                try:
                                    full_df = full_df.union(part_df)
                                    print("union success")
                                except:
                                    pass
                            full_dict[year][quarter][num] = full_df
                            print("add dictionary")
                        except:
                            continue
                    try:
                        print("get from dictionary")
                        full_df = full_dict[year][quarter][num]

                        if full_df is None:
                            logging.info(f"fail file name : {file_name}")
                            self.update_result_df_with_zero(num)
                            continue
                        else:
                            logging.info(f"got file name : {file_name}")
                            df_func, df_calc_func = self.df_func.get_function(num)
                            df_data = df_func(full_df)
                            result_df = df_calc_func(df_data, quarter)

                            result_df.show(10)
                            self.update_result_df(df=result_df)
                            logging.info(f"process success")

                            print("data send start")
                            self.spark.send_file(result_df, f"logs/new/{num}_{year}_{quarter}_report.json")

                    except Exception as e:
                        print(e.__str__())

                    if f"RANK{num}" not in self.result.columns:
                        self.update_result_df_with_zero(num)

                    print("===================================")

                self.update_final_result_df(year, quarter)
                self.reset_result_df()
