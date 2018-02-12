from activeuser import getPAU

from pyspark.sql.functions import *
from pyspark.sql import Window

def getWAU(data, epoch_times, freq, factor, country_list):
    """ Calculates the WAU for dates between start_date and end_date.
        
        Parameters:
        
        data - This should be a sample of the main server ping data frame.
        dates - days (in epoch time) to find WAU for.
        freq - Find the MAU every 'freq' days.
        factor - 100 / percent sample
        country_list - A list of countries that we want to calculate the 
                       MAU for.
                       
        Output:
        
        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids who used pinged between start_date and submission_date_s3.
    """ 
    return getPAU(data, epoch_times, 7, factor, country_list)


def new_users(data, start_date, end_date, factor, country_list):
    """Gets the percentage of WAU that are new users.
    
        Parameters:
        
        data - This should be the entire main server ping data frame.
        start_date - First day to start calculating for
        end_date - last date to calculate for
        factor - 100 / (percent sample)
        country_list - A list of countries that we want to calculate the 
                       PAU for.
    """
    start_day = int(time.mktime(time.strptime(start_date, '%Y%m%d'))) / (60 * 60 * 24 * 7) * 7 * 60 * 60 * 24
    start_date2 = time.strftime('%Y%m%d', time.localtime(start_day))
    end_day = int(time.mktime(time.strptime(end_date, '%Y%m%d'))) / (60 * 60 * 24 * 7) * 7 * 60 * 60 * 24
    dates = range(start_day, end_day, 7 * 60 * 60 * 24)
    
    
    if country_list is None:
        data = data.drop('country').select('*', lit('All').alias('country'))
    else:
        data = data.filter(col('country').isin(country_list))
    
    # We can use count because we know each client_id is unique at that point
    new_users_counts = data.filter((col('submission_date_s3') > start_date) & (col('submission_date_s3') < end_date))\
        .groupBy('client_id', 'country')\
        .agg(min('profile_creation_date').alias('profile_creation_date'))\
        .dropna()\
        .groupBy((col('profile_creation_date') / 7).cast('int').alias('profile_creation_week'), 'country')\
        .agg((factor * count('*')).alias('users_created'))\
        .select('*', from_unixtime(col('profile_creation_week') * 7 * 24 * 60 *60, format='yyyyMMdd').alias('date'))
        
        
    wau = getWAU(data, dates, 7, factor, country_list)\
            .select('*', (unix_timestamp('submission_date_s3', format = 'yyyyMMdd') / (60 * 60 * 24 * 7)).cast('int').alias('profile_creation_week'))
    
    return new_users_counts.join(wau, on = ['profile_creation_week', 'country'], how = 'inner')\
            .select('start_date', 'submission_date_s3', 'country', (col('users_created') / col('active_users')).alias('new_user_rate'))\
            .orderBy('submission_date_s3', 'country')\
            .filter(col('submission_date_s3') != start_date2)
            