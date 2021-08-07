import codecs

from pyspark.sql.functions import count, col, udf
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

from lib.logger import Log4j
from lib.spark_utils import get_spark_session


def main():

    spark = get_spark_session(app_name='Most Popular Movie')

    logger = Log4j(spark.sparkContext)

    movie_name_dict = spark.sparkContext.broadcast(load_movie_names())

    get_movie_name_udf = udf(lambda movie_id: get_movie_name(movie_name_dict, movie_id), StringType())

    logger.info('Reading u.data file')
    ratings_df = spark.read \
        .schema(get_schema()) \
        .option('inferSchema', 'false') \
        .option('header', 'false') \
        .option('delimiter', '\\t') \
        .csv('datasets/ml-100k/u.data')

    logger.info('Grouping, selecting and sorting desired results')
    agg_df = ratings_df \
        .groupBy(ratings_df.movie_id) \
        .agg(
            count(ratings_df.rating).alias('total_ratings')
        ) \
        .select(ratings_df.movie_id, col('total_ratings')) \
        .sort(col('total_ratings'), ascending=False)

    movie_df = agg_df \
        .withColumn('movie_name', get_movie_name_udf(col('movie_id')))\
        .drop(col('movie_id')) \
        .select(col('movie_name'), col('total_ratings'))

    logger.info('Collecting results')
    results = movie_df.collect()

    logger.info('Printing results')
    for result in results:
        print(f'{result[0]} was rated {result[1]} times')

    logger.info('Stopping Spark Session')
    spark.stop()


def get_schema():
    return StructType() \
        .add(StructField('user_id', IntegerType())) \
        .add(StructField('movie_id', IntegerType())) \
        .add(StructField('rating', IntegerType())) \
        .add(StructField('timestamp', LongType()))


def load_movie_names():
    movie_names = {}
    with codecs.open('datasets/ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as lines:
        for line in lines:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]

    return movie_names


def get_movie_name(movie_name_dict, movie_id):
    return movie_name_dict.value[movie_id]
