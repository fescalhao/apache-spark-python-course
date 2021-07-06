import re

from lib.spark_utils import get_spark_context, read_file


def main():
    spark = get_spark_context('Word Count')
    book = read_file(spark, 'datasets/book.txt')
    words = book.flatMap(normalize_words)
    word_count = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    sorted_word_count = word_count.sortBy(lambda x: x[1], False).collect()

    for word, count in sorted_word_count:
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word, count)


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())
