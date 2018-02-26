from pyspark.sql import Window
from pyspark.sql.functions import col, countDistinct, lit, mean, when
import pyspark.sql.functions as F
import pandas as pd


def window_version(os_version):
    """
      Takes the Windows Kernel version number and
      produces the associated consumer windows version.
    """
    return when(os_version == '10.0', 'Windows 10')\
        .when(os_version == '6.1', 'Windows 7')\
        .when(os_version == '6.2', 'Windows 8')\
        .when(os_version == '6.3', 'Windows 8')\
        .when(os_version == '5.1', 'Windows XP')\
        .when(os_version == '6.0', 'Windows Vista')\
        .otherwise('Other Windows')


def nice_os(os, os_version):
    """ Splits the major windows versions up and keeps mac os x and linux combined."""
    return when(os == 'Windows_NT', window_version(os_version))\
        .otherwise(os)

def os_on_date(data, date, country_list):
    """ Gets the distribution of OS usage calculated on the WAU on 1 day.
     
        Parameters:
        data - Usually the main summary data frame
        date - day to get the os distribution for the past week.
        country_list - the countries to do the analysis. If None then it does it for the whole world"""
    start_date = (pd.to_datetime(date, format = '%Y%m%d') - pd.Timedelta(days=7)).strftime('%Y%m%d')
    data = data.select('client_id', 'submission_date_s3', 'country',
                       nice_os(col('os'), col('os_version')).alias('nice_os'))
    
    # Calculate the OS ditribution for the whole world.
    world_wau = data.filter((col('submission_date_s3') <= date) &
                            (col('submission_date_s3') > start_date))\
        .agg(countDistinct('client_id').alias('WAU'))

    world_os_wau = data.filter((col('submission_date_s3') <= date) &
                               (col('submission_date_s3') > start_date))\
        .groupBy('nice_os')\
        .agg(countDistinct('client_id').alias('WAU_on_OS'))\
        .select(lit(start_date).alias('start_date'), lit(date).alias('submission_date_s3'),
                lit('All').alias('country'), 'WAU_on_OS', 'nice_os')
    
    res = world_os_wau.crossJoin(world_wau)
    
    # Calculate the OS distributions for the countries you are intrested in.
    if country_list is not None:
        wau = data.filter(col('country').isin(country_list))\
            .filter((col('submission_date_s3') <= date) & (col('submission_date_s3') > start_date))\
            .groupBy('country')\
            .agg(countDistinct('client_id').alias('WAU'))\
        
        os_wau = data.filter(col('country').isin(country_list))\
            .filter((col('submission_date_s3') <= date) &
                    (col('submission_date_s3') > start_date))\
            .groupBy('country', 'nice_os')\
            .agg(countDistinct('client_id').alias('WAU_on_OS'))\
            .select(lit(start_date).alias('start_date'), lit(date).alias('submission_date_s3'),
                    'country', 'WAU_on_OS', 'nice_os')
            
        
        res = res.union(os_wau.join(wau, 'country')\
                        .select('start_date', 'submission_date_s3', 
                                'country', 'WAU_on_OS', 'nice_os', 'WAU'))
    
    return res.select('country', 'start_date', 'submission_date_s3', col('nice_os').alias('os'),
                       (col('WAU_on_OS') / col('WAU')).alias('ratio_on_os'))

def os_on_dates(data, start_date, end_date, country_list, sc):
    """ Gets the distribution of OS usage calculated on the WAU every 7 days from start_date to end_date (inclusive).
 
        Parameters:
        data - Usually the main summary data frame
        start_date - day to start the analysis
        end_date - last day in the analysis
        country_list - the countries to do the analysis. If None then it does it for the whole world
    """
    
    dates = pd.date_range(pd.to_datetime(start_date, format = '%Y%m%d'), 
                          pd.to_datetime(end_date, format = '%Y%m%d'), freq = '7D')
    
    outs = []
    for date in dates:
        outs.append(os_on_date(data, date.strftime('%Y%m%d'), country_list))
    
    return sc.union([out.rdd for out in outs]).toDF()
