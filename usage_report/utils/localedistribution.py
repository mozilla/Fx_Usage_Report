from pyspark.sql.functions import lit, col, desc, countDistinct
from pyspark.sql import Window
import pyspark.sql.functions as F
from helpers import date_plus_x_days, keep_countries_and_all


def locale_on_date(data, date, topN, period=7, country_list=None):
    """ Gets the ratio of the top locales in each country over the last week.

    parameters:
        data: The main ping server
        date: The date to find the locale distribution
        topN: The number of locales to get for each country. Only does the top N.
        period: The number of days before looked at in the analyisis
        country_list: The list to find look at in the analysis

    output:
       dataframe with columns:
           ['country', 'submission_date_s3', 'locale', 'pct_on_locale']
    """
    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    wau = data_all\
        .filter((col('submission_date_s3') <= date) & (col('submission_date_s3') > begin))\
        .groupBy('country')\
        .agg(countDistinct('client_id').alias('WAU'))

    locale_wau = data_all\
        .filter((col('submission_date_s3') <= date) & (col('submission_date_s3') > begin))\
        .groupBy('country', 'locale')\
        .agg(countDistinct('client_id').alias('WAU_on_locale'))\
        .select(lit(begin).alias('start_date'), lit(date).alias('submission_date_s3'),
                'country', 'WAU_on_locale', 'locale')

    res = locale_wau.join(wau, 'country', how='left')\
        .select('start_date', 'submission_date_s3',
                'country', 'WAU_on_locale', 'locale', 'WAU')

    rank_window = Window.partitionBy('country', 'submission_date_s3').orderBy(desc('WAU_on_locale'))

    return res.select('*', F.row_number().over(rank_window).alias('rank'))\
        .filter(col('rank') <= topN)\
        .select('submission_date_s3', 'country', 'locale',
                (100.0 * col('WAU_on_locale') / col('WAU')).alias('pct_on_locale'))
