from activeuser import getPAU
import time
import datetime as dt
from pyspark.sql.functions import col, lit, countDistinct, from_unixtime


def getWAU(sc, data, date, freq, factor, country_list):
    """ Calculates the WAU for dates between start_date and end_date.

        Parameters:

        data - This should be a sample of the main server ping data frame.
        dates - days (in epoch time) to find WAU for.
        freq - Find the MAU every 'freq' days.
        factor - 100 / percent sample
        country_list - A list of countries that we want to calculate the
                       MAU for.
        sc - Spark context

        Output:

        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids between start_date and submission_date_s3.
    """
    return getPAU(sc, data, date, 7, factor, country_list)


def new_users(sc, data, date, factor, country_list):
    """Gets the percentage of WAU that are new users.

        Parameters:

        data - This should be the entire main server ping data frame.
        date =  data to start calculating for
        factor - 100 / (percent sample)
        country_list - A list of countries that we want to calculate the
                       PAU for.
        sc - Spark context
    """
    day = int(time.mktime(time.strptime(date, '%Y%m%d')))
    cols = ['submission_date_s3', 'client_id', 'profile_creation_date',
            'country']

    wau = getWAU(sc, data, day, 7, factor, country_list)
    df = data.drop('country').select('*', lit('All').alias('country'))

    if country_list is not None:
        df = (
            df.select(cols).union(data.select(cols)
                                  .filter(col('country').isin(country_list))))
    one_week_ago = dt.datetime.fromtimestamp(day - (7 * 24 * 60 * 60)).strftime('%Y%m%d')
    new_profiles = (df.filter(df.submission_date_s3 <= date)
                      .filter(df.submission_date_s3 >= one_week_ago)
                      .withColumn('pcd_str',
                                  from_unixtime(col('profile_creation_date') * 24 * 60 * 60,
                                                format='yyyyMMdd'))
                      .filter(col('pcd_str') <= date)
                      .filter(col('pcd_str') >= one_week_ago))

    if new_profiles.count() == 0:
        new_user_counts = (
            df.select('country').distinct()
              .select('*', lit(0).alias('new_users')))
    else:
        new_user_counts = (
              new_profiles
              .groupBy('country')
              .agg((factor * countDistinct('client_id')).alias('new_users')))

    return (
        # use left join to ensure we always have the same number of countries
        wau.join(new_user_counts, on=['country'], how='left')
        .selectExpr('*', 'new_users / WAU as new_user_rate')
        )
