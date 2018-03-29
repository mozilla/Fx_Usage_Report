from helpers import date_plus_x_days, keep_countries_and_all

from pyspark.sql.functions import lit
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
            'avg_daily_usage(hours)'
    """

    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    data_agg = data_all\
        .filter("submission_date_s3 <= '{}' and submission_date_s3 > '{}'"
                .format(date, begin))\
        .filter("subsession_length <= 86400") .filter("subsession_length > 0")\
        .groupBy('country',
                 'client_id',
                 'submission_date_s3')\
        .agg(F.sum('subsession_length').alias('total_daily_time'))\
        .select('country',
                'client_id',
                'submission_date_s3',
                F.when(F.col('total_daily_time') > 86400, 86400)
                 .otherwise(F.col('total_daily_time'))
                 .alias('total_daily_time'))

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

    return df.select('submission_date_s3', 'country', 'avg_daily_usage(hours)')
