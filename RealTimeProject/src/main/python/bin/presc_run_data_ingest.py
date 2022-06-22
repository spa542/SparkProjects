import logging
import logging.config


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info('The load_files() function is started...')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).options(header=header).options(inferSchem=inferSchema).load(file_dir)
    except Exception as e:
        logger.error('Error in the method - load_files(). Please check the stack trace.')
        raise
    else:
        logger.info(f'The input file {file_dir} is loaded to the dataframe. The load_files() function is complete.\n\n')
    return df

