#!/usr/bin/env scala
import findspark

findspark.init()

from configure import get_file, get_spark
from dataframe import seoul_three

print("start spark")


spark = get_spark()
print("===================")


df_spark = get_file(spark, "s3a://open-rtc-data/test_three.csv")
print("===================")

new_one = seoul_three(df_spark)
new_one.show(2)
