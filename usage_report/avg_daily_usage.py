import time
import datetime
import pandas as pd

from pyspark.sql.functions import avg, col, lit


def getDailyAvgSession(
        data,
        date,
        period=7,
        country_list=None,
        locale_list=None):
    """ Calculate Average Daily usage of the last 7 days for a particular date

        Parameters:
        data: sample of the main server ping data frame
        date: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        localelist: a list of locale information in strings

        Returns:
        a dataframe with four columns:
            'submission_date_s3',
            'country',
            'avg_daily_subsession_length',
            'avg_daily_usage(hours)'
    """
    enddate = datetime.datetime.strptime(date, '%Y%m%d')
    periodstartdate = enddate - datetime.timedelta(days=period)
    periodstartdate_str = periodstartdate.strftime('%Y%m%d')

    data1 = data\
        .filter("submission_date_s3 <= '{}' and submission_date_s3 > '{}'"
                .format(date, periodstartdate_str))\
        .filter("subsession_length <= 86400") .filter("subsession_length > 0")\
        .groupBy('country',
                 'client_id',
                 'submission_date_s3')\
        .agg(sum('subsession_length').alias('total_daily_time'))

    worldAvgSession = data1.groupBy('client_id')\
        .agg(avg('total_daily_time').alias('client_7d_avg'))\
        .agg(avg('client_7d_avg').alias('avg_daily_subsession_length'))\
        .select(lit(date).alias('submission_date_s3'), lit('All').alias('country'), '*')
    df = worldAvgSession

    if country_list is not None:
        countryAvgSession = data1.filter(col('country').isin(country_list))\
            .groupBy('country', 'client_id')\
            .agg(avg('total_daily_time').alias('client_7d_avg'))\
            .groupBy('country')\
            .agg(avg('client_7d_avg').alias('avg_daily_subsession_length'))\
            .select(lit(date).alias('submission_date_s3'), '*')
        df = worldAvgSession.union(countryAvgSession).orderBy(
            'submission_date_s3', 'country')

    df = df.withColumn(
        'avg_daily_usage(hours)',
        df.avg_daily_subsession_length / 3600)

    return df


def getAvgSession(sc, data, start_date, end_date, country_list, locale_list=None):
    """ Calculate Average Daily usage for a period of dates

        Parameters:
        data: sample of the main server ping data frame
        startdate: string, with the format of 'yyyyMMdd'
        enddate: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        localelist: a list of locale information in strings

        Returns:
        a dataframe showing all the information for each date in the period
          'submission_date_s3',
          'country',
          'avg_daily_subsession_length',
          'avg_daily_usage(hours)'
    """
    res = []
    epoch_times = pd.date_range(
        start_date, end_date, freq='D').astype('int64') / (10 ** 9)
    for date in epoch_times:
        date_str = time.strftime('%Y%m%d', time.localtime(date))
        current_date = getDailyAvgSession(
            data,
            date_str,
            period=7,
            country_list=country_list,
            locale_list=None)
        res = res + [current_date]
    return sc.union([df.rdd for df in res]).toDF()
