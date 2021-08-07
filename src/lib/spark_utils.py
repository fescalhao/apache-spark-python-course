import configparser
import os
from pyspark import SparkContext, SparkConf, RDD
from pyspark.sql import SparkSession


def get_spark_context(app_name: str) -> SparkContext:
    spark_conf = get_spark_config(app_name)
    return SparkContext(conf=spark_conf)


def get_spark_session(app_name: str) -> SparkSession:
    spark_conf = get_spark_config(app_name)
    return SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()


def read_file_rdd(spark: SparkContext, path: str) -> RDD:
    return spark.textFile(os.path.abspath(path))


def get_spark_config(app_name: str) -> SparkConf:
    spark_conf = SparkConf().setAppName(app_name)
    config = configparser.ConfigParser()
    config.read('spark.conf')

    for (k, v) in config.items('SPARK_LOCAL_CONFIGS'):
        spark_conf.set(k, v)

    return spark_conf
