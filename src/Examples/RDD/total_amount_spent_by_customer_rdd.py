from Examples.spark_utils import get_spark_context, read_file


def main():
    spark = get_spark_context("Total Amount Spent By Customer")
    lines = read_file(spark, 'datasets/customer-orders.csv')
    rdd = lines.map(parse_line)

    total_spent = rdd \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], ascending=False) \
        .collect()

    for customer in total_spent:
        print(customer[0].rjust(2, ' ') + ' -> ${:.2f}'.format(customer[1]))


def parse_line(line: str):
    fields = line.split(',')
    customer = fields[0]
    amount = float(fields[2])
    return customer, amount
