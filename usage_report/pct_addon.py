import datetime
import pandas as pd
import json
import urllib
import time

# from pyspark.sql.functions import col, lit, mean, split
import pyspark.sql.functions as F

def get_test_pilot_addons():
    '''
    Fetches all the live test pilot experiments listed in
    the experiments.json file. 
    
    returns a list of addon_ids
    '''
    url = "https://testpilot.firefox.com/api/experiments.json"
    response = urllib.urlopen(url)
    data = json.loads(response.read())
    all_tp_addons = ["@testpilot-addon"] + [i.get("addon_id") for i in data['results'] if i.get("addon_id")]
    return all_tp_addons


# grab all tp addons without a mozilla suffix
NON_MOZ_TP = [i for i in get_test_pilot_addons() if "@mozilla" not in i]

# this study is everywhere
UNIFIED_SEARCH_STR = '@unified-urlbar-shield-study-'


def getAddon(data, 
             date, 
             period = 7, 
             country_list = None):
    """ Calculate the proportion of WAU that have a "self installed" addon for a specific date
        Parameters:
        data: sample of the main server ping data frame
        date: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        Returns:
        a dataframe showing all the information for each date in the period
          - five columns: 'submission_date_s3', 'country', 'WAU', 'add_on_count', 'pct_Addon'
    """
    enddate = datetime.datetime.strptime(date, '%Y%m%d')
    periodstartdate = enddate - datetime.timedelta(days=period)
    periodstartdate_str = periodstartdate.strftime('%Y%m%d')
    
    
    if country_list is None:
        data1 = data.drop('country').select('submission_date_s3', 'client_id', 'active_addons'
                                            , F.lit('All').alias('country'))
    else:
        data2 = data.filter(F.col('country').isin(country_list))\
                    .select('submission_date_s3', 'client_id', 'active_addons', 'country')
        data1 = data.drop('country').select('submission_date_s3', 'client_id', 'active_addons',
                                            F.lit('All').alias('country'))\
                    .union(data2)
    
    addon_filter = (~F.col('addon.is_system')) & (~F.col('addon.foreign_install')) & \
                    (~F.col('addon.addon_id').isin(NON_MOZ_TP)) & (~F.col('addon.addon_id').like('%@mozilla%')) &\
                    (~F.col('addon.addon_id').like('%@shield.mozilla%')) &\
                    (~F.col('addon.addon_id').like('%' + UNIFIED_SEARCH_STR + '%'))
    
    WAU = data1.filter("submission_date_s3 <= '%s' and submission_date_s3 > '%s'"%(date, periodstartdate_str))\
                .groupBy('country')\
                .agg(F.countDistinct('client_id').alias('WAU'))
    
    AddonCount = data1.filter("submission_date_s3 <= '%s' and submission_date_s3 > '%s'"%(date, periodstartdate_str))\
                .select('submission_date_s3', 'country', 'client_id', F.explode('active_addons').alias('addon'))\
                .filter(addon_filter)\
                .groupBy('country')\
                .agg(F.countDistinct('client_id').alias('add_on_count'))
                
    join_df = WAU.join(AddonCount, 'country')\
                .withColumn("pct_Addon", (F.col("add_on_count") / F.col("WAU")))\
                .select(F.lit(date).alias('submission_date_s3'), '*')
    return join_df

def getPeriodAddon(
        sc,
        data,
        start_date,
        end_date,
        country_list):
    """ Calculate the proportion of WAU that have a "self installed" addon for a period of dates
        Parameters:
        data: sample of the main server ping data frame
        startdate: string, with the format of 'yyyyMMdd'
        enddate: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        Returns:
        a dataframe showing all the information for each date in the period
          - five columns: 'submission_date_s3', 'country', 'WAU', 'add_on_count', 'pct_Addon'
    """
    res = []
    epoch_times = pd.date_range(
        start_date, end_date, freq='D').astype('int64') / (10 ** 9)
    for date in epoch_times:
        date_str = time.strftime('%Y%m%d', time.localtime(date))
        current_date = getAddon(data,
                                date_str,
                                period=7,
                                country_list=country_list,
                                locale_list=None)
        res = res + [current_date]
    return sc.union([df.rdd for df in res]).toDF()

