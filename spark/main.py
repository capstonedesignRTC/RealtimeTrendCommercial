import logging
import sys

from modules.calculation import Calculate
from spark.spark_configure import SparkS3

if __name__ == "__main__":
    logging.error("start spark process")

    spark = SparkS3()

    print("===================")
    calculate = Calculate(spark)
    calculate.insert_specific_args(sys.argv[1:])
    result = calculate.calculation_all_founds()

    print("===================")

