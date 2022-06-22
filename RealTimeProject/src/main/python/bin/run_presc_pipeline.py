### Import all necessary modules
import sys
import os
import logging
import logging.config
import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top10_rec, df_print_schema
from presc_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_prescribers
from presc_run_data_extraction import extract_files
from subprocess import Popen, PIPE


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')


def main():
    try:
        logging.info('main() is started...')
        ### Get Spark object
        spark = get_spark_object(gav.envn, gav.appName)
        # Validate Spark object
        get_curr_date(spark)

        ### Initiate run_presc_data_ingest script
        # Load the city file
        if os.environ['envn'] == 'TEST':
            for file in os.listdir(gav.staging_dim_city):
                logging.info(f'File is {file}')
                # For local windows
                file_dir = gav.staging_dim_city + '/' + file
                # For prod
                file_dir = gav.prod_staging + '/' + file
                logging.info(f'File directory is {file_dir}')
                if file.split('.')[1] == 'csv':
                    file_format = 'csv'
                    header = gav.header
                    inferSchema = gav.inferSchema
                elif file.split('.')[1] == 'parquet':
                    file_format = 'parquet'
                    header = 'NA'
                    inferSchema = 'NA'
        elif os.environ['envn'] == 'PROD':
            file_dir = 'PrescPipeline/staging/dimension_city'
            proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
            (out, err) = proc.communicate()
            if 'parquet' in out.decode():
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif 'csv' in out.decode():
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

        # Validate run_data_ingest script for city dimension dataframe
        df_count(df_city, 'df_city')
        df_top10_rec(df_city, 'df_city')

        if os.environ['envn'] == 'TEST':
            # Load the prescriber fact file
            for file in os.listdir(gav.staging_fact):
                logging.info(f'File is {file}')
                # For windows
                #file_dir = gav.staging_fact + '/' + file
                # For prod
                file_dir = gav.prod_staging + '/' + file
                logging.info(f'File directory is {file_dir}')
                if file.split('.')[1] == 'csv':
                    file_format = 'csv'
                    header = gav.header
                    inferSchema = gav.inferSchema
                elif file.split('.')[1] == 'parquet':
                    file_format = 'parquet'
                    header = 'NA'
                    inferSchema = 'NA'
        elif os.environ['envn'] == 'PROD':
            file_dir = 'PrescPipeline/staging/fact'
            proc = Popen(['hdfs', 'dfs', '-ls', '-C', file_dir], stdout=PIPE, stderr=PIPE)
            (out, err) = proc.communicate()
            if 'parquet' in out.decode():
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif 'csv' in out.decode():
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

        # Validate run_data_ingest script for fact dimension dataframe
        df_count(df_fact, 'df_fact')
        df_top10_rec(df_fact, 'df_fact')

        ### Initiate run_presc_data_preprocessing script
        # Perform data cleaning operations
        #1 Select only required columns
        #2 Convert city, state, and county fields to upper case
        # Perform data cleaning for df_fact
        #1 Select only required columns
        #2 Rename the columns
        #3 Add a country field 'USA'
        #4 Clean years_of_exp field
        #5 Convert the years_of_exp datatype from string to number
        #6 Combine first name and last name
        #7 Check and clean all the null/nan values
        #8 Impute (fill) TRX_CNT where it is null as avg of TRX_CNT for the prescriber
        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)
        # Validate for df_city_sel and df_fact_sel
        df_top10_rec(df_city_sel, 'df_city_sel')
        df_top10_rec(df_fact_sel, 'df_fact_sel')
        df_print_schema(df_city_sel, 'df_city_sel')
        df_print_schema(df_fact_sel, 'df_fact_sel')

        ### Initiate run_presc_data_transform script
        # Apply all the transformation logic
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_prescribers(df_fact_sel)
        # Validate
        df_top10_rec(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')
        df_top10_rec(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')

        ### Initiate run_data_extraction script
        CITY_PATH = gav.output_city
        extract_files(df_city_final, 'json', CITY_PATH, 1, False, 'bzip2')
        PRESC_PATH = gav.output_fact
        extract_files(df_presc_final, 'orc', PRESC_PATH, 2, False, 'snappy')

        logging.info('presc_run_pipeline.py is completed.')
    except Exception as e:
        logging.error('Error occured in main() method. Please check the stack trace to go to the respective module and fix it - ' + str(e), exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logging.info('run_presc_pipeline.py is started...')
    main()

