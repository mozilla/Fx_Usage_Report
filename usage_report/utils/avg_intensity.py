from helpers import date_plus_x_days

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F


def get_avg_intensity(data, date, period=7, country_list=None):
    """ Calculate Average Intensity of the last 7 days for a particular date

        Parameters:
        data: sample of the main server ping data frame
        date: string, with the format of 'yyyyMMdd'
        country_list: a list of country names in string

        Returns:
        a dataframe with three columns: 'submission_date_s3', 'country', 'avg_intensity'
    """
    data_all = data.drop('country')\
        .select('submission_date_s3', 'client_id', 'subsession_length', 'active_ticks',
                F.lit('All').alias('country'))

    if country_list is not None:
        data_countries = data.filter(F.col('country').isin(country_list))\
            .select('submission_date_s3', 'client_id', 'subsession_length',
                    'active_ticks', 'country')

        data_all = data_all.union(data_countries)

    begin = date_plus_x_days(date, -period)

    data_agg = data_all\
        .filter("submission_date_s3 <= '{0}' and submission_date_s3 > '{1}'".format(date, begin))\
        .filter("subsession_length <= 86400")\
        .filter("subsession_length > 0")\
        .filter('active_ticks <= 17280')\
        .groupBy('country', 'client_id', 'submission_date_s3')\
        .agg(F.sum('subsession_length').alias('total_daily_time'),
             F.sum('active_ticks').alias('total_daily_ticks'))\
        .select('*',
                (col('total_daily_ticks') * 5 / col('total_daily_time'))
                .alias('avg_daily_intensity'))

    country_avg_intensity = data_agg\
        .groupBy('country', 'client_id')\
        .agg(F.avg('avg_daily_intensity').alias('avg_7d_intensity'))\
        .groupBy('country')\
        .agg(F.avg('avg_7d_intensity').alias('avg_intensity'))\
        .select(lit(date).alias('submission_date_s3'), '*')

    df = country_avg_intensity.orderBy('submission_date_s3', 'country')

    return df
