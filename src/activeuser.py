import time
import datetime

from pyspark.sql.functions import *
from pyspark.sql import Window


def getPAU(data, epoch_times, period, factor, country_list):
    """ Calculates the PAU for a given period for each time in epoch_times.
        
        This function is fast for finding the PAU for a small number of dates.
        
        Paramaters:
        
        data - This should be a sample of the main server ping data frame.
        epoch_times - The epoch time of each day you want to find the PAU for
        period - The number of days that we count distinct number of users.
                 For example MAU has a period = 28 and YAU has a period = 365.
                 
        factor - 100 / percent sample
        
        country_list - A list of countries that we want to calculate the 
                       PAU for.
                       
        Output:
        
        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids who used pinged between start_date and submission_date_s3.
    """
    res = []
    
    if country_list is None:
        data = data.drop('country').select('*', lit('All').alias('country'))
    else:
        data = data.filter(col('country').isin(country_list))
    
    for date in epoch_times:
        begin = date - period * 24 * 60 * 60
        begin_str = time.strftime('%Y%m%d', time.localtime(begin))

        date_str = time.strftime('%Y%m%d', time.localtime(date))
        
        current_count = data.filter(col('submission_date_s3') > begin_str)\
                .filter(col('submission_date_s3') <= date_str)\
                .groupBy('country')\
                .agg((factor * countDistinct('client_id')).alias('active_users'))\
                .select(lit(begin_str).alias('start_date'), lit(date_str).alias('submission_date_s3'), '*')

        res = res + [current_count]
    
    return sc.union([df.rdd for df in res]).toDF()


def getMAU(data, start_date, end_date, freq, factor, country_list):
    """ Calculates the MAU for dates between start_date and end_date.
        
        Parameters:
        
        data - This should be a sample of the main server ping data frame.
        start_date - The first day to start calculating the MAU for.
        end_date - The last day to start calculating the MAU for.
        freq - Find the MAU every 'freq' days.
        factor - 100 / percent sample
        country_list - A list of countries that we want to calculate the 
                       MAU for.
                       
        Output:
        
        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids who used pinged between start_date and submission_date_s3.
    """
    begin = int(time.mktime(time.strptime(start_date, '%Y%m%d')))
    end = int(time.mktime(time.strptime(end_date, '%Y%m%d')))
    
    epoch_times = range(begin, end, freq * 24 * 60 * 60)
        
    return getPAU(data, epoch_times, 28, factor, country_list)


def getYAU(data, start_date, end_date, factor, country_list):
    """ Calculates the YAU for dates between start_date and end_date. 
        This function calculates the YAU on the first of each month between 
        start_date and end_date.
    
        Parameters:
        
        data - This should be a sample of the main server ping data frame.
        start_date - The first day to start calculating the YAU for.
        end_date - The last day to start calculating the YAU for.
        factor - 100 / percent sample
        country_list - A list of countries that we want to calculate the 
                       MAU for.
        
        
        Output:
        
        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids who used pinged between start_date and submission_date_s3.
    """

    begin = int(time.mktime(time.strptime(start_date, '%Y%m%d')))
    end = int(time.mktime(time.strptime(end_date, '%Y%m%d')))
    
    epoch_times = range(begin, end, 7 * 24 * 60 * 60)
    
    return getPAU(data, epoch_times, 365, factor, country_list)