from pyspark import SparkContext


class Log4j:
    """Wrapper class for Log4j JVM object.
    :param spark: SparkContext object.
    """

    def __init__(self, spark: SparkContext):
        log4j = spark._jvm.org.apache.log4j
        root_class = 'src'
        conf = spark.getConf()
        app_name = conf.get('spark.app.name')
        self.logger = log4j.LogManager.getLogger(root_class + '.' + app_name)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log an warning.
        :param: Error message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None