import sys

import findspark

findspark.init()
import logging

from modules.caculation import Calculate
from modules.dataframe import get_sk_code_to_hjd_code
from spark.spark_configure import SparkS3

if __name__ == "__main__":
    logging.error("start spark process")

    spark = SparkS3(sys.argv)

    print("===================")
    convert_code_csv = spark.get_file("convert_code.csv")
    code_dict = get_sk_code_to_hjd_code(convert_code_csv)

    logging.error("creating code_dict complete")

    print("===================")
    calculate = Calculate(spark, code_dict)

    calculate.insert_specific_args([])

    logging.error(f"GOT specific_args")
    result = calculate.calculation_all_founds()

    print("===================")

    # for idx, arg in enumerate(sys.argv[1:]):
    #     if idx == 0:
    #         city_input = int(arg)
    #     else:
    #         specific_args.append(str(arg))

    # # if city_input is not None:
    # #     if city_input // 10000 == 1:
    # #         print("big")
    # #         """big code만 필요"""
    # #         result = calculate.calculation_big_cities(city_input)
    # #     elif city_input // 10000000 == 1:
    # #         print("middle")
    # #         """big code도 필요"""
    # #         big = calculate.find_city_code(city_input, True)
    # #         result = calculate.calculation_middle_cities(big, city_input)
    # #     else:
    # #         """big, middle code도 필요"""
    # #         print("small")
    # #         big, middle = calculate.find_city_code(city_input, False)
    # #         result = calculate.calculation_small_cities(big, middle, city_input)
    # # else:
    # #     print("all")
    # #     result = calculate.calculation_all_cities()

    # print("===================")

