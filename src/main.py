from DataFrame import *

# -------------- RDD Examples --------------
# ratings_histogram_rdd.main()
# average_friends_by_age_rdd.main()
# minimum_temp_by_location_rdd.main()
# word_count_rdd.main()
# total_amount_spent_by_customer_rdd.main()
# ------------------------------------------

# ----------- DataFrame Examples -----------
# average_friends_by_age_df.main()
# minimum_temp_by_location_df.main()
# word_count_df.main()
# total_amount_spent_by_customer_df.main()
# ------------------------------------------
from lib.logger import Log4j
from lib.spark_utils import get_spark_session

if __name__ == '__main__':
    # spark = get_spark_session('Bla')
    # logger = Log4j(spark.sparkContext)
    # logger.info('Reading fakefriends.csv file')
    average_friends_by_age_df.main()
