import logging
import logging.config
from pyspark.sql.functions import upper, size, countDistinct, sum, dense_rank, col
from pyspark.sql.window import Window
from udfs import column_split_cnt


### Load the configuration file
logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger = logging.getLogger(__name__)


def city_report(df_city_sel, df_fact_sel):
    """
    City Report:
        Transform logics:
            1. Calculate the number of zips in each city
            2. Calculate the number of distinct prescribers assigned for each city
            3. Calculate total TRX_CNT prescribed for each city
            4. Do not report a city in the final report if no prescriber is assigned to it

        Layout:
            City Name
            State Name
            County Name
            City Population
            Number of Zips
            Prescriber Counts
            Total Trx Counts
    """
    try:
        logger.info('Transform - city_report() function started...')
        df_city_split = df_city_sel.withColumn('zip_counts', column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(countDistinct('presc_id').alias('presc_counts'), sum('trx_cnt').alias('trx_counts'))
        df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) & (df_city_split.city == df_fact_grp.presc_city), 'inner')
        df_city_final = df_city_join.select('city', 'state_name', 'county_name', 'population', 'zip_counts', 'trx_counts', 'presc_counts')
    except Exception as e:
        logger.error('Error in the method - city_report(). Please check the stack trace. ' + str(e), exc_info=True)
        raise
    else:
        logger.info('Transform - city_report() function is complete.')
    return df_city_final


def top_5_prescribers(df_fact_sel):
    """
    Prescriber Report:
        Top 5 prescribers with highest trx_cnt per each state. Consider the prescribers only from 20 to 50
        years of experience.
        Layout:
            Prescriber ID
            Prescriber Full Name
            Prescriber State
            Prescriber Country
            Prescriber Years of Experience
            Total TRX Count
            Total Days Supply
            Total Drug Cost
    """
    try:
        logger.info('Transform - top_5_prescribers() is started...')
        spec = Window.partitionBy('presc_state').orderBy(col('trx_cnt').desc())
        df_presc_final = df_fact_sel.select('presc_id', 'presc_fullname', 'presc_state', 'country_name', 'years_of_exp', 'trx_cnt', 'total_day_supply', 'total_drug_cost') \
            .filter((df_fact_sel.years_of_exp >= 20) & (df_fact_sel.years_of_exp <= 50)) \
            .withColumn('dense_rank', dense_rank().over(spec)) \
            .filter(col('dense_rank') <= 5) \
            .select('presc_id', 'presc_fullname', 'presc_state', 'country_name', 'years_of_exp', 'trx_cnt', 'total_day_supply', 'total_drug_cost')
    except Exception as e:
        logger.error('Error in the method - top_5_prescribers(). Please check the stack trace. ' + str(e), exc_info=True)
        raise
    else:
        logger.info('Transform - top_5_prescribers() is complete.\n\n')
    return df_presc_final

