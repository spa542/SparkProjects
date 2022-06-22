import logging
import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, when, isnan, avg, round, coalesce
from pyspark.sql.window import Window


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def perform_data_clean(df1, df2):
    ### Clean df_city dataframe:
    # Select only required columns
    # Apply upper function for columns - city, state, and county
    ### Clean df_fact dataframe:
    #1 Select only required columns
    #2 Rename the columns
    #3 Add a country field 'USA'
    #4 Clean years_of_exp field
    #5 Convert the years_of_exp datatype from string to number
    #6 Combine first name and last name
    #7 Check and clean all the null/nan values
    #8 Impute (fill) TRX_CNT where it is null as avg of TRX_CNT for that prescriber
    # - Delete the records where the presc_id is NULL
    # - Delete the records where the drug_name is null
    try:
        logger.info('perform_data_clean() is started...')
        logger.info('Data cleaning for df_city_sel has started...')
        df_city_sel = df1.select(upper(df1.city).alias('city'),
                                 df1.state_id,
                                 upper(df1.state_name).alias('state_name'),
                                 upper(df1.county_name).alias('county_name'),
                                 df1.population,
                                 df1.zips) # 1 and 2
        logger.info('Data cleaning for df_fact_sel has started...')
        df_fact_sel = df2.select(df2.npi.alias('presc_id'),
                                 df2.nppes_provider_last_org_name.alias('presc_lname'),
                                 df2.nppes_provider_first_name.alias('presc_fname'),
                                 df2.nppes_provider_city.alias('presc_city'),
                                 df2.nppes_provider_state.alias('presc_state'),
                                 df2.specialty_description.alias('presc_spclt'),
                                 df2.years_of_exp,
                                 df2.drug_name,
                                 df2.total_claim_count.alias('trx_cnt'),
                                 df2.total_day_supply,
                                 df2.total_drug_cost) # 1 and 2
        df_fact_sel = df_fact_sel.withColumn('country_name', lit('USA')) # 3
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn('years_of_exp', regexp_extract(col('years_of_exp'), pattern, idx)) # 4
        df_fact_sel = df_fact_sel.withColumn('years_of_exp', col('years_of_exp').cast('int')) # 5
        df_fact_sel = df_fact_sel.withColumn('presc_fullname', concat_ws(' ', 'presc_fname', 'presc_lname')) # 6
        df_fact_sel = df_fact_sel.drop('presc_fname', 'presc_lname') # 6
        # df_fact_sel = df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns])
        df_fact_sel = df_fact_sel.dropna(subset='presc_id') # 8
        df_fact_sel = df_fact_sel.dropna(subset='drug_name') # 8
        spec = Window.partitionBy('presc_id') # 8
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce('trx_cnt', round(avg('trx_cnt').over(spec)))) # 8
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', col('trx_cnt').cast('int')) # 8
    except Exception as e:
        logger.error('Error in the method - perform_data_clean(). Please check the stack trace. ' + str(e), exc_info=True)
        raise
    else:
        logger.info('perform_data_clean() is complete.\n\n')
    return df_city_sel, df_fact_sel


