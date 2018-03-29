from helpers import date_plus_x_days, keep_countries_and_all

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F


def get_avg_intensity(data, date, period=7, country_list=None):
    """ Calculate Average Intensity of the last 7 days for a particular date

        Parameters:
        data: sample of the main server ping data frame
        date: string, with the format of 'yyyyMMdd'
        period: The number of days before to run the analysis on.
        country_list: a list of country names in string

        Returns:
        a dataframe with three columns: 'submission_date_s3', 'country', 'avg_intensity'
    """
    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    data_agg = data_all\
        .filter("submission_date_s3 <= '{0}' and submission_date_s3 > '{1}'".format(date, begin))\
        .filter("subsession_length <= 86400")\
        .filter("subsession_length > 0")\
        .filter('active_ticks <= 17280')\
        .groupBy('country', 'client_id', 'submission_date_s3')\
        .agg(F.sum('subsession_length').alias('total_daily_time'),
             F.sum('active_ticks').alias('total_daily_ticks'))\
        .select('country',
                'client_id',
                'submission_date_s3',
                F.when(F.col('total_daily_time') > 86400, 86400)
                 .otherwise(F.col('total_daily_time'))
                 .alias('total_daily_time'),
                F.when(F.col('total_daily_ticks') > 17280, 17280)
                 .otherwise(F.col('total_daily_ticks'))
                 .alias('total_daily_ticks'))\
        .select('*',
                (col('total_daily_ticks') * 5 / col('total_daily_time'))
                .alias('daily_intensity'))\
        .select('country',
                'client_id',
                'submission_date_s3',
                F.when(F.col('daily_intensity') > 1, 1)
                 .otherwise(F.col('daily_intensity'))
                 .alias('daily_intensity'))

    country_avg_intensity = data_agg\
        .groupBy('country', 'client_id')\
        .agg(F.avg('daily_intensity').alias('avg_7d_intensity'))\
        .groupBy('country')\
        .agg(F.avg('avg_7d_intensity').alias('avg_intensity'))\
        .select(lit(date).alias('submission_date_s3'), '*')

    df = country_avg_intensity.orderBy('submission_date_s3', 'country')

    return df
