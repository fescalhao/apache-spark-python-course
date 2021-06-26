import os
from pyspark import SparkContext, SparkConf, RDD


def get_spark_context(app_name: str) -> SparkContext:
    spark_conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName(app_name)

    return SparkContext(conf=spark_conf)


def read_file(spark: SparkContext, path: str) -> RDD:
    return spark.textFile(os.path.abspath(path))