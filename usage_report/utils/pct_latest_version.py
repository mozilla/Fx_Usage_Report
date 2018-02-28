import datetime
import pandas as pd
import json
import urllib

from pyspark.sql.functions import col, mean, split
import pyspark.sql.functions as F
from helpers import date_plus_x_days, keep_countries_and_all

RELEASE_VERSIONS_URL = "https://product-details.mozilla.org/1.0/firefox_history_major_releases.json"


def get_release_df(spark, data, url):
    """ Generate a dataframe with the latest release version on each date
        Parameters:
        spark: a spark session
        data: sample of the main server ping data frame
        url: path to the json file containing all the firefox release information to date
        Returns:
        a dataframe with four columns:
            'submission_date_s3',
            'latest_version',
            'release_date',
            'is_release_date'
    """
    # retrieve all the distinct submission date from main ping
    submission_date_s3 = data.select('submission_date_s3').distinct().orderBy(
        'submission_date_s3').toPandas()

    # load data from firefox_history_major_releases.json
    response = urllib.urlopen(url)
    jrelease = json.loads(response.read())
    release_df = pd.DataFrame({'version': jrelease.keys(),
                              'date': pd.Categorical(jrelease.values())})

    release_df['date'] = release_df['date'].str.replace('-', '')
    release_df_ordered = release_df.sort_values('date').reset_index(drop=True)
    release_df_ordered['date_next'] = release_df_ordered['date'].shift(-1)

    # fill in the NA value on the last date of release_df_ordered['date_next']
    today = submission_date_s3['submission_date_s3'].max()
    last = datetime.datetime.strptime(
        today, '%Y%m%d') + datetime.timedelta(days=1)
    last_str = last.strftime('%Y%m%d')
    release_df_ordered_filled = release_df_ordered.fillna(value=last_str)

    # cross join the submission_date and release_df_ordered_filled table
    submission_date_s3['tmp'] = 1
    release_df_ordered_filled['tmp'] = 1
    df = pd.merge(submission_date_s3, release_df_ordered_filled, on=['tmp'])
    df = df.drop('tmp', axis=1)

    # filter data to show the lastesst release version on each date
    release_version = df[(df['submission_date_s3'] >= df['date']) & (
        df['submission_date_s3'] < df['date_next'])].reset_index(drop=True)
    release_version = release_version[['submission_date_s3', 'version', "date"]]
    release_version.columns = [
        u'submission_date_s3',
        u'latest_version',
        u'release_date'
    ]

    # add a column showing whether it's release date on each day
    release_version['is_release_date'] = 0
    release_version.loc[
        release_version['submission_date_s3'] == release_version['release_date'],
        'is_release_date'
    ] = 1

    # convert release_date pandas df to spark df
    release_date = spark.createDataFrame(release_version)
    release_date = release_date.withColumn(
        'latest_version',
        split('latest_version', '\.').getItem(0)
    )
    return release_date


def pct_new_version(data,
                  date,
                  country_list=None,
                  period = 7,
                  url=RELEASE_VERSIONS_URL,
                  **kwargs):
    """ Calculate the proportion of active users on the latest release version in the last 7-day period.

        Parameters:
        data: sample of the main server ping data frame
        date: The day to calculate the metric
        country_list: a list of country names in string
        period: number of days to use to calculate metric
        url: path to the json file containing all the firefox release information to date
        Returns:
        a dataframe with five columns - 'submission_date_s3', 'country', 'latest_version_count',
                                        'pct_latest_version', 'is_released_by_week'

    """

    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    release_date = get_release_df(kwargs['spark'], data, url)
    data_filtered = data_all.withColumn('app_major_version', split('app_version', '\.').getItem(0))\
        .select('submission_date_s3',
                'client_id',
                'app_major_version',
                'country')\
        .filter("{0} >= '{1}' and {0} <= '{2}'"
                .format("submission_date_s3", begin, date))

    joined_df = data_filtered\
        .join(release_date,
              data_filtered.submission_date_s3 == release_date.submission_date_s3,
              'inner')\
        .drop(release_date.submission_date_s3)

    latest_version_by_week = joined_df.agg(F.max('latest_version').alias('latest_version_by_week'))
    joined_df = joined_df.crossJoin(latest_version_by_week).drop('latest_version')
    
    new_ver_country = joined_df\
        .groupBy('country', 'client_id')\
        .agg(F.max(col('app_major_version') == col('latest_version_by_week'))
             .cast('int').alias('is_latest'),
             F.max('is_release_date').alias('is_release_date'))\
        .groupBy('country')\
        .agg(F.sum('is_latest').alias('latest_version_count'),
             mean('is_latest').alias('pct_latest_version'),
             F.max('is_release_date').alias('is_released_by_week'))\
        .orderBy('country')\
        .select(F.lit(date).alias('submission_date_s3'), '*')

    return new_ver_country
  