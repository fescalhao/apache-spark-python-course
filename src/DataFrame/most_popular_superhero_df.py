from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, split, size, sum, encode, broadcast
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from lib.logger import Log4j
from lib.spark_utils import get_spark_session


def main():
    spark = get_spark_session(app_name='MostPopularSuperhero')

    logger = Log4j(spark.sparkContext)

    logger.info('Getting superhero_df')
    superhero_df = get_superhero_df(spark, logger)

    logger.info('Getting superhero_names_df')
    superhero_names_df = get_superhero_names_df(spark, logger)

    logger.info('Defining join condition')
    cond = superhero_df.superhero_id == superhero_names_df.superhero_id

    logger.info('Joining DataFrames to get superhero name instead of superhero id and sorting results')
    most_popular_df = superhero_df.join(broadcast(superhero_names_df), cond, how='inner') \
        .select(
            superhero_names_df.superhero_name,
            superhero_df.co_appearances
        ) \
        .sort(superhero_df.co_appearances, ascending=False)

    logger.info('Showing top 20 results for most popular superheros')
    most_popular_df.show(20, truncate=False)

    logger.info('Stopping Spark Session')
    spark.stop()


def get_marvel_name_schema():
    return StructType() \
        .add(StructField('superhero_id', IntegerType())) \
        .add(StructField('superhero_name', StringType()))


def get_superhero_df(spark: SparkSession, logger: Log4j) -> DataFrame:
    logger.info('Reading marvel_graph.txt file')
    marvel_graph_df = spark.read \
        .text('datasets/marvel_graph.txt')

    logger.info('Defining columns for superhero_id and number of co-appearances with other superheros')
    superhero_df = marvel_graph_df \
        .withColumn('superhero_id', split(col('value'), ' ')[0]) \
        .withColumn('co_appearances', size(split(col('value'), ' ')) - 1) \
        .select(col('superhero_id'), col('co_appearances')) \
        .alias('superhero_df')

    logger.info('Grouping co-appearances by superhero_id')
    grouped_superhero_df = superhero_df \
        .groupBy(superhero_df.superhero_id) \
        .agg(
            sum(superhero_df.co_appearances).alias('co_appearances')
        ).alias('superhero_df')

    logger.info('Returning superhero_df')
    return grouped_superhero_df


def get_superhero_names_df(spark: SparkSession, logger: Log4j) -> DataFrame:
    logger.info('Reading marvel_names.txt file')
    marvel_names_df = spark.read \
        .schema(get_marvel_name_schema()) \
        .option('inferSchema', 'false') \
        .option('header', 'false') \
        .option('delimiter', ' ') \
        .csv('datasets/marvel_names.txt')

    logger.info('Selecting superhero_id and superhero_name with the right encode')
    superhero_names_df = marvel_names_df.select(
        marvel_names_df.superhero_id,
        encode(marvel_names_df.superhero_name, 'iso-8859-1').cast(StringType()).alias('superhero_name')
    ).alias('superhero_names_df')

    logger.info('Returning superhero_names_df')
    return superhero_names_df
