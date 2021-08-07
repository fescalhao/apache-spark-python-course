from pyspark.sql.functions import col, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from lib.logger import Log4j
from lib.spark_utils import get_spark_session


def main():
    spark = get_spark_session(app_name='Minimum Temperature By Location')

    logger = Log4j(spark.sparkContext)

    logger.info('Reading 1800.csv file')
    df = spark.read \
        .option('inferSchema', 'false') \
        .option('header', 'false') \
        .schema(get_temp_schema()) \
        .csv('datasets/1800.csv')

    logger.info('Filtering, grouping, selecting and ordering desired results')
    min_temp_df = df \
        .filter(df.measure_type == 'TMIN') \
        .groupBy(df.station_id) \
        .agg(
            min(df.temperature).alias('min_temperature')
        ) \
        .select(df.station_id, col('min_temperature')) \
        .sort(col('min_temperature'), ascending=True)

    logger.info('Collecting results')
    results = min_temp_df.collect()

    logger.info('Print results')
    for result in results:
        print(result[0] + ' -> {:.2f}'.format(result[1]))

    logger.info('Stopping Spark Session')
    spark.stop()


def get_temp_schema():
    return StructType() \
        .add(StructField('station_id', StringType())) \
        .add(StructField('date', IntegerType())) \
        .add(StructField('measure_type', StringType())) \
        .add(StructField('temperature', FloatType()))
