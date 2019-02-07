import pyspark.sql.functions as F
from pyspark.sql.functions import lit, col, desc
from pyspark.sql import Window
import json
from helpers import date_plus_x_days, keep_countries_and_all


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


def top_10_addons_on_date(data, date, topN, period=7, country_list=None):
    """ Gets the number of users in the past week who have used the top N addons,
        broken down by country.

        Parameters:
        data - The main ping server.
        date - The day you which you want to get the top N addons.
        topN - the number of addons to get.
        period - number of days to use to calculate metric
        country_list - a list of country names in string

        Returns:
        Dataframe containing the number of users using each of the addons.
        submission_date_s3, country, addon_id, name, percent_of_active_users
    """
    addon_filter = (~col('addon.is_system')) & (~col('addon.foreign_install')) & \
        (~col('addon.addon_id').isin(NON_MOZ_TP)) & (~col('addon.addon_id').like('%@mozilla%')) &\
        (~col('addon.addon_id').like('%@shield.mozilla%')) &\
        (~col('addon.addon_id').like('%' + UNIFIED_SEARCH_STR + '%'))

    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)

    wau = data_all.filter((col('submission_date_s3') > begin) &
                          (col('submission_date_s3') <= date))\
        .groupBy('country')\
        .agg(lit(date).alias('submission_date_s3'),
             F.countDistinct('client_id').alias('wau'))

    counts = data_all.select('submission_date_s3', 'country', 'client_id',
                             F.explode('active_addons').alias('addon'))\
        .filter((col('submission_date_s3') > begin) &
                (col('submission_date_s3') <= date))\
        .filter(addon_filter)\
        .select('country', 'client_id', 'addon.addon_id', 'addon.name')\
        .distinct()\
        .groupBy('country', 'addon_id')\
        .agg(F.count('*').alias('number_of_users'), F.last('name').alias('name'))\
        .select('*', lit(date).alias('submission_date_s3'),
                lit(begin).alias('start_date'),
                F.row_number().over(Window.partitionBy('country')
                                    .orderBy(desc('number_of_users'))
                                    .rowsBetween(Window.unboundedPreceding, Window.currentRow))
                              .alias('rank'))\
        .filter(col('rank') <= topN)

    return counts.join(F.broadcast(wau), on=['country'], how='left')\
        .select(lit(date).alias('submission_date_s3'), 'country',
                'addon_id', col('name').alias('addon_name'),
                (100.0 * col('number_of_users') / col('wau')).alias('pct_with_addon'))
