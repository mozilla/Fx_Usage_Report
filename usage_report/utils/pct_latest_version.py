import datetime
import json
import urllib

from pyspark.sql.functions import split
import pyspark.sql.functions as F
from helpers import date_plus_x_days, keep_countries_and_all

RELEASE_VERSIONS_URL = "https://product-details.mozilla.org/1.0/firefox_history_major_releases.json"


def get_latest_version(date, url):
    """ check a url and get the latest release given a date
        Param:
        date: date in question. should be YYYYMMDD format (str)
        url: url where the Firefox release history json lives
        Return: release major version for that date (50, not 50.0)
    """
    date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
    response = urllib.urlopen(url)
    jrelease = json.loads(response.read())
    jrelease = dict((v, k) for k, v in jrelease.iteritems())
    last_update = max([release_date for release_date in jrelease.keys() if release_date <= date])
    return jrelease[last_update].split('.')[0]


def pct_new_version(data,
                    date,
                    period=7,
                    country_list=None,
                    url=RELEASE_VERSIONS_URL):
    """ Calculate the proportion of active users on the latest release version every day.
        Parameters:
        data: sample of the main server ping data frame
        date: The day to calculate the metric
        period: number of days to use to calculate metric
        country_list: a list of country names in string
        url: path to the json file containing all the firefox release information to date
        Returns:
        a dataframe with five columns - 'country', 'submission_date_s3',
                                        'pct_latest_version'
    """

    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    latest_version = get_latest_version(date, url)
    data_filtered = data_all.filter("""
                                    {0} >= '{1}' and {0} <= '{2}'
                                    """.format("submission_date_s3", begin, date))\
                            .withColumn('app_major_version',
                                        split('app_version',
                                              r'\.').getItem(0))\
                            .select('submission_date_s3',
                                    'client_id',
                                    'app_major_version',
                                    'country')

    WAU = data_filtered.groupBy('country')\
                       .agg(F.countDistinct('client_id').alias('WAU'))
    WAU_latest = data_filtered.filter(F.col('app_major_version') >= F.lit(latest_version))\
                              .groupBy('country')\
                              .agg(F.countDistinct('client_id').alias('WAU_is_latest'))
    join_df = WAU.join(WAU_latest, 'country', 'left')\
                 .withColumn("pct_latest_version", (100.0 * F.col("WAU_is_latest") / F.col("WAU")))\
                 .select(F.lit(date).alias('submission_date_s3'),
                         'country',
                         F.coalesce('pct_latest_version', F.lit(0)).alias('pct_latest_version'))
    return join_df
