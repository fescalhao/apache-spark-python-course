import os.path
from typing import Tuple
from Examples.spark_utils import get_spark_context, read_file


def main():
    spark = get_spark_context('Minimum Temperature By Location')
    lines = read_file(spark, os.path.abspath('datasets/1800.csv'))
    rdd = lines.map(lambda line: parse_line(line))

    min_rdd = rdd \
        .filter(lambda x: x[1] == 'TMIN') \
        .map(lambda x: (x[0], x[2])) \
        .reduceByKey(lambda x, y: min(x, y)) \
        .collect()

    for temp in min_rdd:
        print(temp[0] + "\t{:.2f}°C".format(temp[1]))


def parse_line(line: str) -> Tuple[str, str, float]:
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temp = float(fields[3])
    return station_id, entry_type, temp
