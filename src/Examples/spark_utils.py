import os
from pyspark import SparkContext, SparkConf, RDD
from pyspark.sql import SparkSession


def get_spark_context(app_name: str) -> SparkContext:
    spark_conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName(app_name)

    return SparkContext(conf=spark_conf)


def get_spark_session(app_name: str) -> SparkSession:
    spark_conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName(app_name)

    return SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()


def read_file(spark: SparkContext, path: str) -> RDD:
    return spark.textFile(os.path.abspath(path))
