import logging
import sys

from modules.calculation import Calculate
from spark.configure import SparkS3

if __name__ == "__main__":
    logging.info("start spark process")

    spark = SparkS3()
    calculate = Calculate(spark)
    calculate.insert_specific_args(sys.argv[1:])
    print("===================")
    result = calculate.calculation_all_founds()
    calculate.spark.stop_spark()
    print("===================")

