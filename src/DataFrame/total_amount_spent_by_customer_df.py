from pyspark.sql.functions import sum, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

from lib.logger import Log4j
from lib.spark_utils import get_spark_session


def main():

    spark = get_spark_session(app_name='Total Amount Spent By Customer')

    logger = Log4j(spark.sparkContext)

    logger.info('Reading customer-orders.csv file')
    df = spark.read \
        .schema(get_schema()) \
        .option('inferSchema', 'false') \
        .option('header', 'false') \
        .csv('datasets/customer-orders.csv')

    logger.info('Grouping, selecting and sorting desired results')
    total_df = df \
        .groupBy(df.customer_id) \
        .agg(
            sum(df.value).alias('total_spent')
        ) \
        .select(df.customer_id, col('total_spent')) \
        .sort(col('total_spent'), ascending=False)

    logger.info('Collecting results')
    results = total_df.collect()

    logger.info('Print results')
    for result in results:
        print(f'Customer {str(result[0]).rjust(2, "0")} spent a total of ${"{:.2f}".format(result[1])}')

    logger.info('Stopping Spark Session')
    spark.stop()


def get_schema():
    return StructType() \
        .add(StructField('customer_id', IntegerType())) \
        .add(StructField('product_id', IntegerType())) \
        .add(StructField('value', FloatType()))
