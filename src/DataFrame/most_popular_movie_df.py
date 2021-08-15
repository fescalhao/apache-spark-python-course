import codecs

from pyspark.sql.functions import count, col, udf, broadcast, encode
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

from lib.logger import Log4j
from lib.spark_utils import get_spark_session


def main():
    spark = get_spark_session(app_name='Most Popular Movie')

    logger = Log4j(spark.sparkContext)

    # Example broadcasting a dict to the cluster
    # movie_name_dict = spark.sparkContext.broadcast(load_movie_names())
    # get_movie_name_udf = udf(lambda movie_id: get_movie_name(movie_name_dict, movie_id), StringType())

    logger.info('Reading u.data file')
    ratings_df = spark.read \
        .schema(get_rating_schema()) \
        .option('inferSchema', 'false') \
        .option('header', 'false') \
        .option('delimiter', '\\t') \
        .csv('datasets/ml-100k/u.data')

    logger.info('Reading u.item file')
    item_df = spark.read \
        .schema(get_movie_schema()) \
        .option('inferSchema', 'false') \
        .option('header', 'false') \
        .option('delimiter', '|') \
        .csv('datasets/ml-100k/u.item')

    logger.info('Grouping, selecting and sorting desired results for ratings DataFrame')
    agg_df = ratings_df \
        .groupBy(ratings_df.movie_id) \
        .agg(
            count(ratings_df.rating).alias('total_ratings')
        ) \
        .select(ratings_df.movie_id, col('total_ratings')) \
        .sort(col('total_ratings'), ascending=False) \
        .alias('agg_df')

    logger.info('Selecting movie_id and movie_name encoded to iso-8859-1')
    movie_name_df = item_df.select(
        item_df.movie_id,
        encode(item_df.movie_name, 'iso-8859-1').cast(StringType()).alias('movie_name')
    ).alias('movie_name_df')

    logger.info('Defining join condition')
    cond = agg_df.movie_id == movie_name_df.movie_id

    logger.info('Joining agg_df and movie_name_df to get the movie_name instead of movie_id')
    movie_df = agg_df.join(broadcast(movie_name_df), cond, how='inner') \
        .select(movie_name_df.movie_name, agg_df.total_ratings)

    logger.info('Collecting results')
    results = movie_df.collect()

    logger.info('Printing results')
    for result in results:
        print(f'{result[0]} was rated {result[1]} times')

    logger.info('Stopping Spark Session')
    spark.stop()


def get_rating_schema():
    return StructType() \
        .add(StructField('user_id', IntegerType())) \
        .add(StructField('movie_id', IntegerType())) \
        .add(StructField('rating', IntegerType())) \
        .add(StructField('timestamp', LongType()))


def get_movie_schema():
    return StructType() \
        .add(StructField('movie_id', IntegerType())) \
        .add(StructField('movie_name', StringType())) \
        .add(StructField('release_date', StringType()))

# Example broadcasting a dict to the cluster
# def load_movie_names():
#     movie_names = {}
#     with codecs.open('datasets/ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as lines:
#         for line in lines:
#             fields = line.split('|')
#             movie_names[int(fields[0])] = fields[1]
#
#     return movie_names
#
#
# def get_movie_name(movie_name_dict, movie_id):
#     return movie_name_dict.value[movie_id]
