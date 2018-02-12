import time
import datetime
import numpy as np
import pandas as pd
import json

from pyspark.sql.functions import *
from pyspark.sql import Window

# ! wget https://product-details.mozilla.org/1.0/firefox_history_major_releases.json 
filepath = '/home/hadoop/analyses/firefox_history_major_releases.json'

def get_realease_df(data, filepath):
    """ Generate a dataframe with the latest release version on each date
              
        Parameters:
        data: sample of the main server ping data frame, filtered with 
              normalized_channel:release, sample_id:42, subsession_length between 0 and 86400(inclusive)
        filepath: path to the json file containing all the firefox release information to date

        Returns: 
        a dataframe with four columns - 'submission_date_s3', 'latest_version','release_date', 'is_release_date'
    """
    # retrieve all the distinct submission date from main ping
    submission_date_s3 = data.select('submission_date_s3').distinct().orderBy('submission_date_s3').toPandas()

    # load data from firefox_history_major_releases.json 
    jrelease = json.load(file(filepath)
    release_df = pd.DataFrame({'version' : jrelease.keys(), 'date' : pd.Categorical(jrelease.values())})
    release_df['date'] = release_df['date'].str.replace('-', '')
    release_df_ordered = release_df.sort_values('date').reset_index(drop = True)
    release_df_ordered['date_next'] = release_df_ordered['date'].shift(-1)

    # fill in the NA value on the last date of release_df_ordered['date_next'] 
    today = submission_date_s3['submission_date_s3'].max()
    last = datetime.datetime.strptime(today, '%Y%m%d') + datetime.timedelta(days=1)
    last_str = last.strftime('%Y%m%d')
    release_df_ordered_filled = release_df_ordered.fillna(value = last_str)

    # cross join the submission_date and release_df_ordered_filled table
    submission_date_s3['tmp'] = 1
    release_df_ordered_filled['tmp'] = 1
    df = pd.merge(submission_date_s3, release_df_ordered_filled, on=['tmp'])
    df = df.drop('tmp', axis=1)

    # filter data to show the lastesst release version on each date 
    release_version = df[(df['submission_date_s3']>= df['date']) & (df['submission_date_s3'] < df['date_next'])].reset_index(drop = True)
    release_version = release_version[['submission_date_s3','version',"date"]]
    release_version.columns = [u'submission_date_s3', u'latest_version', u'release_date']

    # add a column showing whether it's release date on each day
    release_version['is_release_date'] = 0
    release_version.loc[release_version['submission_date_s3']==release_version['release_date'],'is_release_date'] = 1

    #convert release_date pandas df to spark df
    release_date = sqlContext.createDataFrame(release_version)
    release_date = release_date.withColumn('latest_version', split('latest_version', '\.').getItem(0))
    return release_date
                         
def pctnewversion(data, filepath, startdate, enddate, countrylist = None, localelist = None):
    """ Calculates the proportion of active users on the latest release version every day.
              
        Parameters:
        data: sample of the main server ping data frame, filtered with 
              normalized_channel:release, sample_id:42, subsession_length between 0 and 86400(inclusive)
        filepath: path to the json file containing all the firefox release information to date
        startdate: string, with the format of 'yyyyMMdd'
        enddate: string, with the format of 'yyyyMMdd'
        countrylist: a list of country names in string
        localelist: a list of locale information in strings

        Returns: 
        a dataframe with five columns - 'country', 'submission_date_s3', 'lastest_version_count',
                                        'pct_latest_version', 'is_release_date'
    """
    release_date = get_realease_df(data, filepath)                     
    data1 = data.withColumn('app_major_version', split('app_version', '\.').getItem(0))\
                .select('submission_date_s3','client_id','app_major_version','country')\
                .filter("submission_date_s3 >= '%s' and submission_date_s3 <= '%s'"%(startdate, enddate))
    joined_df = data1.join(release_date, data1.submission_date_s3 == release_date.submission_date_s3, 'inner')\
                .drop(release_date.submission_date_s3)
    
    newverglobal = joined_df.groupBy('submission_date_s3','client_id')\
        .agg(max(col('app_major_version') == col('latest_version')).cast('int').alias('is_latest'),
             max('is_release_date').alias('is_release_date'))\
        .groupBy('submission_date_s3')\
        .agg(sum('is_latest').alias('lastest_version_count'),mean('is_latest').alias('pct_latest_version'),
             max('is_release_date').alias('is_release_date'))\
        .orderBy('submission_date_s3').select(lit('All').alias('country'), '*')
    df = newverglobal
    
    if countrylist is not None:
        newvercountry = joined_df.where(col('country').isin(countrylist))\
            .groupBy('country','submission_date_s3','client_id')\
            .agg(max(col('app_major_version') == col('latest_version')).cast('int').alias('is_latest'),
                 max('is_release_date').alias('is_release_date'))\
            .groupBy('country','submission_date_s3')\
            .agg(sum('is_latest').alias('lastest_version_count'),mean('is_latest').alias('pct_latest_version'),
                max('is_release_date').alias('is_release_date'))\
            .orderBy('submission_date_s3', 'country')
        df = newverglobal.union(newvercountry).orderBy('submission_date_s3', 'country')
    return df