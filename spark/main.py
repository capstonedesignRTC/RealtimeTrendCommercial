import findspark

findspark.init()
import logging
import sys

from caculation import Calculate
from dataframe import get_sk_code_to_hjd_code
from spark.spark_configure import SparkS3

if __name__ == "__main__":
    spark = SparkS3()

    c = Calculate(spark, {})

    c.test(3)

    # logging.error("start spark process")
    # spark = SparkS3()

    # print("===================")

    # convert_code_csv = spark.get_file_test("utils/convert_code.csv")
    # code_dict = get_sk_code_to_hjd_code(convert_code_csv)
    # logging.error("creating code_dict complete")

    # print("===================")

    # calculate = Calculate(spark, code_dict)

    # city_input, specific_args = None, []

    # for idx, arg in enumerate(sys.argv[1:]):
    #     if idx == 0:
    #         city_input = int(arg)
    #     else:
    #         specific_args.append(str(arg))

    # if city_input is not None:
    #     if city_input // 10000 == 1:
    #         result = calculate.calculation_big_cities(city_input, specific_args)
    #     elif city_input // 10000000 == 1:
    #         result = calculate.calculation_middle_cities(city_input, specific_args)
    #     else:
    #         result = calculate.calculation_small_cities(city_input, specific_args)
    # else:
    #     result = calculate.calculation_all_cities(specific_args)

    # print("===================")
    # result.show(2)

    # # # df_spark = spark.get_file(spark, "s3a://open-rtc-data/test_three.csv")
    # # # new_one = seoul_three(df_spark)
    # # # new_one.show(2)
