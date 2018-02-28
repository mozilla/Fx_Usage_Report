from activeuser import getPAU
from pyspark.sql.functions import col, lit, countDistinct, from_unixtime
from helpers import date_plus_x_days


def getWAU(data, date, country_list):
    """ Helper function for getPAU with period 7 days.
    """
    return getPAU(data, date, 7, country_list)


def new_users(data, date, country_list, period=7):
    """Gets the percentage of WAU that are new users.

        Parameters:

        data - This should be the entire main server ping data frame.
        date =  data to start calculating for
        country_list - A list of countries that we want to calculate the
                       PAU for.
    """

    cols = ['submission_date_s3', 'client_id', 'profile_creation_date',
            'country']

    wau = getWAU(data, date, country_list)
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

    if new_profiles.count() == 0:
        new_user_counts = (
            df.select('country').distinct()
              .select('*', lit(0).alias('new_users')))
    else:
        new_user_counts = (
              new_profiles
              .groupBy('country')
              .agg((100 * countDistinct('client_id')).alias('new_users')))

    return (
        # use left join to ensure we always have the same number of countries
        wau.join(new_user_counts, on=['country'], how='left')
        .selectExpr('*', 'new_users / WAU as new_user_rate')
        )
