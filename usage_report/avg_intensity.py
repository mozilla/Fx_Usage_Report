import time
import datetime
import pandas as pd

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F


def getAvgIntensity(data, date, period=7, country_list=None, locale_list=None):
    """ Calculate Average Intensity of the last 7 days for a particular date

        Parameters:
        data: sample of the main server ping data frame
        date: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        localelist: a list of locale information in strings

        Returns:
        a dataframe with three columns: 'submission_date_s3', 'country', 'avg_intensity'
    """
    date_colname = "submission_date_s3"
    enddate = datetime.datetime.strptime(date, '%Y%m%d')
    periodstartdate = enddate - datetime.timedelta(days=period)
    periodstartdate_str = periodstartdate.strftime('%Y%m%d')

    data1 = data\
        .filter("{0} <= '{1}' and {0} > '{2}'".format(date_colname, date, periodstartdate_str))\
        .filter("subsession_length <= 86400")\
        .filter("subsession_length > 0")\
        .filter('active_ticks <= 17280')\
        .groupBy('country', 'client_id', 'submission_date_s3')\
        .agg(F.sum('subsession_length').alias('total_daily_time'),
             F.sum('active_ticks').alias('total_daily_ticks'))\
        .select('*',
                (col('total_daily_ticks') * 5 / col('total_daily_time'))
                .alias('avg_daily_intensity'))

    worldAvgIntensity = data1.groupBy('client_id') .agg(
        F.avg('avg_daily_intensity').alias('avg_7d_intensity')) .agg(
        F.avg('avg_7d_intensity').alias('avg_intensity')) .select(
            lit(date).alias('submission_date_s3'),
            lit('All').alias('country'),
        '*')
    df = worldAvgIntensity
    if country_list is not None:
        countryAvgIntensity = data1.filter(col('country').isin(country_list))\
            .groupBy('country', 'client_id')\
            .agg(F.avg('avg_daily_intensity').alias('avg_7d_intensity'))\
            .groupBy('country')\
            .agg(F.avg('avg_7d_intensity').alias('avg_intensity'))\
            .select(lit(date).alias('submission_date_s3'), '*')
        df = worldAvgIntensity.union(countryAvgIntensity).orderBy(
            'submission_date_s3', 'country')
    return df


def getPeriodAvgIntensity(
        sc,
        data,
        start_date,
        end_date,
        country_list,
        locale_list=None):
    """ Calculate Average Intensity for a period of dates

        Parameters:
        data: sample of the main server ping data frame
        startdate: string, with the format of 'yyyyMMdd'
        enddate: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        localelist: a list of locale information in strings

        Returns:
        a dataframe showing all the information for each date in the period
          - three columns: 'submission_date_s3', 'country', 'avg_intensity'
    """
    res = []
    epoch_times = pd.date_range(
        start_date, end_date, freq='D').astype('int64') / (10 ** 9)
    for date in epoch_times:
        date_str = time.strftime('%Y%m%d', time.localtime(date))
        current_date = getAvgIntensity(
            data,
            date_str,
            period=7,
            country_list=country_list,
            locale_list=None)
        res = res + [current_date]
    return sc.union([df.rdd for df in res]).toDF()
