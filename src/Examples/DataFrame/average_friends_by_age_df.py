from pyspark import Row
from pyspark.sql.functions import avg, round, desc

from Examples.spark_utils import get_spark_session, read_file


def main():
    spark = get_spark_session('Average Friends By Age')
    lines = read_file(spark.sparkContext, 'datasets/fakefriends.csv')
    people = lines.map(mapper)

    df_people = spark.createDataFrame(people).cache()

    avg_friends = df_people \
        .select('age', 'num_friends') \
        .groupBy('age') \
        .agg(round(avg('num_friends'), 2).alias('avg_friends')) \
        .sort(desc('avg_friends'))

    avg_friends.show()
    spark.stop()


def mapper(line: str) -> Row:
    fields = line.split(',')
    id = int(fields[0])
    name = fields[1].encode('utf-8')
    age = int(fields[2])
    friends = int(fields[3])
    return Row(id=id, name=name, age=age, num_friends=friends)
