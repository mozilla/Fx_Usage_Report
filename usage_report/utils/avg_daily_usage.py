import time
import datetime
from helpers import date_plus_x_days

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F


def get_daily_avg_session(
        data,
        date,
        period=7,
        country_list=None):
    """ Calculate Average Daily usage of the last 7 days for a particular date

        Parameters:
        data: sample of the main server ping data frame
        date: string, with the format of 'yyyyMMdd'
        country_list: a list of country names in string

        Returns:
        a dataframe with four columns:
            'submission_date_s3',
            'country',
            'avg_daily_subsession_length',
            'avg_daily_usage(hours)'
    """

    data_all = data.drop('country')\
                    .select('submission_date_s3', 'client_id', 'subsession_length', 
                            F.lit('All').alias('country'))

    if country_list is not None:
        data_countries = data.filter(F.col('country').isin(country_list))\
                    .select('submission_date_s3', 'client_id', 'subsession_length', 'country')

        data_all = data_all.union(data_countries)

    begin = date_plus_x_days(date, -period)

    data_agg = data_all\
        .filter("submission_date_s3 <= '{}' and submission_date_s3 > '{}'"
                .format(date, begin))\
        .filter("subsession_length <= 86400") .filter("subsession_length > 0")\
        .groupBy('country',
                 'client_id',
                 'submission_date_s3')\
        .agg(F.sum('subsession_length').alias('total_daily_time'))\

    country_avg_session = data_agg\
        .groupBy('country', 'client_id')\
        .agg(F.avg('total_daily_time').alias('client_7d_avg'))\
        .groupBy('country')\
        .agg(F.avg('client_7d_avg').alias('avg_daily_subsession_length'))\
        .select(lit(date).alias('submission_date_s3'), '*')

    df = country_avg_session.orderBy(
        'submission_date_s3', 'country')

    df = df.withColumn(
        'avg_daily_usage(hours)',
        df.avg_daily_subsession_length / 3600)

    return df
