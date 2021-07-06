from pyspark.sql.functions import explode, split, col, lower
from pyspark.sql.types import StructType, StringType

from lib.spark_utils import get_spark_session


def main():
    spark = get_spark_session('Word Count')

    schema = StructType().add('lines', StringType(), nullable=True)

    book = spark.read \
        .option('header', 'false') \
        .schema(schema=schema) \
        .text('datasets/book.txt')

    words = book.select(split(lower(book.lines), '\\W+').alias('words')) \
        .withColumn('words', explode('words')) \
        .filter(col('words') != '') \
        .groupBy('words') \
        .count() \
        .sort('count', ascending=False)

    words.show()

