from typing import Tuple
from lib.spark_utils import get_spark_context, read_file


def main():
    spark = get_spark_context('Average Friends By Age')
    lines = read_file(spark, 'datasets/fakefriends.csv')
    rdd = lines.map(parse_line)

    total_by_age = rdd \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    average_by_age = total_by_age.mapValues(lambda x: round((x[0] / x[1]), 2)).sortByKey()

    result = average_by_age.collect()

    for row in result:
        print(row)


def parse_line(line: str) -> Tuple[int, int]:
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])

    return age, friends
