from pyspark import Row
from pyspark.sql.functions import avg, round, desc

from lib.logger import Log4j
from lib.spark_utils import get_spark_session, read_file


def main():
    spark = get_spark_session('Average Friends By Age')

    logger = Log4j(spark.sparkContext)

    logger.info('Reading fakefriends.csv file')
    lines = read_file(spark.sparkContext, 'datasets/fakefriends.csv')

    logger.info('Mapping necessary fields')
    people = lines.map(mapper)

    logger.info('Creating a DataFrame')
    df_people = spark.createDataFrame(people).cache()

    logger.info('Calculating the average frinds by age')
    avg_friends = df_people \
        .select('age', 'num_friends') \
        .groupBy('age') \
        .agg(round(avg('num_friends'), 2).alias('avg_friends')) \
        .sort(desc('avg_friends'))

    logger.info('Printing results')
    avg_friends.show()

    logger.info('Stopping spark session')
    spark.stop()


def mapper(line: str) -> Row:
    fields = line.split(',')
    id = int(fields[0])
    name = fields[1].encode('utf-8')
    age = int(fields[2])
    friends = int(fields[3])
    return Row(id=id, name=name, age=age, num_friends=friends)
