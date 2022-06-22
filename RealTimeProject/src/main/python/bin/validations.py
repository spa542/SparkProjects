import logging
import logging.config
import pandas as pd


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def get_curr_date(spark):
    try:
        opDF = spark.sql(""" select current_date """)
        logger.info(f'Validate the Spark object by printing current date - {str(opDF.collect())}')
    except NameError as e:
        logger.error('NameError in the method - spark_curr_date(). Please check the stack trace. ' + str(e))
        raise
    except Exception as e:
        logger.error('Error in the method - spark_curr_date(). Please check the stack trace. ' + str(e))
    else:
        logger.info('Spark object is validated. Spark object is ready\n\n')


def df_count(df, df_name):
    try:
        logger.info(f'The dataframe validation by count df_count() is started for dataframe {df_name}...')
        df_count = df.count()
        logger.info(f'The dataframe count is {df_count}.')
    except Exception as e:
        logger.error('Error in the method - df_count(). Please check the stack trace. ' + str(e))
        raise
    else:
        logger.info(f'The dataframe validation by count df_count() is complete.\n\n')


def df_top10_rec(df, df_name):
    try:
        logger.info(f'The dataframe validation by top 10 record of df_top10_rec() is started for dataframe {df_name}...')
        logger.info(f'The dataframe top 10 records are: ')
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as e:
        logger.error('Error in the method - df_top10_rec(). Please check the stack trace. ' + str(e))
        raise
    else:
        logger.info('The dataframe validation by top 10 record df_top10_rec() is complete.\n\n')


def df_print_schema(df, df_name):
    try:
        logger.info(f'The dataframe schema validation for dataframe {df_name}...')
        sch = df.schema.fields
        logger.info(f'The dataframe {df_name} schema is: ')
        for i in sch:
            logger.info(f'\t{i}')
    except Exception as e:
        logger.error('Error in the method - df_print_schema(). Please check the stack trace. ' + str(e))
        raise
    else:
        logger.info('The dataframe schema validation is complete.\n\n')


