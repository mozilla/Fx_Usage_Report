from activeuser import getPAU
from pyspark.sql.functions import col, lit, countDistinct, from_unixtime
from helpers import date_plus_x_days


def getWAU(data, date, country_list=None):
    """ Helper function for getPAU with period 7 days.
    """
    return getPAU(data, date, period=7, country_list=country_list)


def new_users(data, date, period=7, country_list=None):
    """Gets the percentage of WAU that are new users.

        Parameters:

        data - This should be the entire main server ping data frame.
        date -  data to start calculating for
        period - The number of days before looked at in the analysis
        country_list - A list of countries that we want to calculate the
                       PAU for.

        Returns:
          A dataframe with columns
            submission_date_s3, country, pct_new_users
    """

    cols = ['submission_date_s3', 'client_id', 'profile_creation_date',
            'country']

    wau = getWAU(data, date, country_list=country_list)
    df = data.drop('country').select('*', lit('All').alias('country'))

    if country_list is not None:
        df = (
            df.select(cols).union(data.select(cols)
                                  .filter(col('country').isin(country_list))))
    begin = date_plus_x_days(date, -period)
    new_profiles = (df.filter(df.submission_date_s3 <= date)
                      .filter(df.submission_date_s3 > begin)
                      .withColumn('pcd_str',
                                  from_unixtime(col('profile_creation_date') * 24 * 60 * 60,
                                                format='yyyyMMdd'))
                      .filter(col('pcd_str') <= date)
                      .filter(col('pcd_str') > begin))

    new_user_counts = (
          new_profiles
          .groupBy('country')
          .agg((countDistinct('client_id')).alias('new_users')))

    return wau.join(new_user_counts, on=['country'], how='left')\
              .select('submission_date_s3',
                      'country',
                      (100.0 * col('new_users') / col('WAU')).alias('pct_new_user'))
