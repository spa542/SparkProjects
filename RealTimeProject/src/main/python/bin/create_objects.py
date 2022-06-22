from pyspark.sql import SparkSession
import logging
import logging.config


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def get_spark_object(envn, appName):
    try:
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
    except NameError as e:
        logger.error('NameError in the method - get_spark_object(). Please check the stack trace. ' + str(e), exc_info=True)
        raise
    except Exception as e:
        logger.error('Error in the method - get_spark_object(). Please check the stack trace. ' + str(e), exc_info=True)
    else:
        logger.info('Spark object is created...\n\n')
    return spark

