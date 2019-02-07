import json
from helpers import date_plus_x_days, keep_countries_and_all

# from pyspark.sql.functions import col, lit, mean, split
import pyspark.sql.functions as F


def get_test_pilot_addons():
    '''
    Fetches all the live test pilot experiments listed in
    the experiments.json file.
    returns a list of addon_ids
    '''
    file_path = "usage_report/resources/experiments.json"
    with open(file_path) as f:
        data = json.load(f)
    all_tp_addons = ["@testpilot-addon"] + [i.get("addon_id")
                                            for i in data['results']
                                            if i.get("addon_id")]
    return all_tp_addons


# grab all tp addons without a mozilla suffix
NON_MOZ_TP = [i for i in get_test_pilot_addons() if "@mozilla" not in i]

# this study is everywhere
UNIFIED_SEARCH_STR = '@unified-urlbar-shield-study-'


def get_addon(data,
              date,
              period=7,
              country_list=None):
    """ Calculate the proportion of WAU that have a "self installed" addon for a specific date

        Parameters:
            data: sample of the main server ping data frame
            date: string, with the format of 'yyyyMMdd'
            period: The number of days before looked at in the analysis
            country_list: a list of country names in string

        Returns:
            a dataframe showing all the information for each date in the period
              - three columns: 'submission_date_s3', 'country', 'pct_Addon'
    """

    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    addon_filter = (~F.col('addon.is_system')) & (~F.col('addon.foreign_install')) &\
                   (~F.col('addon.addon_id').isin(NON_MOZ_TP)) &\
                   (~F.col('addon.addon_id').like('%@mozilla%')) &\
                   (~F.col('addon.addon_id').like('%@shield.mozilla%')) &\
                   (~F.col('addon.addon_id').like('%' + UNIFIED_SEARCH_STR + '%'))

    WAU = data_all\
        .filter("submission_date_s3 <= '{0}' and submission_date_s3 > '{1}'".format(date, begin))\
        .groupBy('country')\
        .agg(F.countDistinct('client_id').alias('WAU'))

    addon_count = data_all\
        .filter("submission_date_s3 <= '{0}' and submission_date_s3 > '{1}'".format(date, begin))\
        .select('submission_date_s3', 'country', 'client_id',
                F.explode('active_addons').alias('addon'))\
        .filter(addon_filter)\
        .groupBy('country')\
        .agg(F.countDistinct('client_id').alias('add_on_count'))

    join_df = WAU.join(addon_count, 'country', how='left')\
        .withColumn("pct_addon", (100.0 * F.col("add_on_count") / F.col("WAU")))\
        .select(F.lit(date).alias('submission_date_s3'), '*')

    return join_df.select('submission_date_s3', 'country', 'pct_addon')
