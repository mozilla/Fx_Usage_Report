from pyspark.sql.functions import *
from pyspark.sql import Window

def localeDistribution(data, start_date, end_date, country_list, spark, num_locales):
    """ Gets the locale distribution for the given date range. Returns only the top num_locale
        locales.
    
        Parameters:
        data - Usually the main summary data frame
        start_date - day to start the analysis
        end_date - last day in the analysis
        country_list - the countries to do the analysis. If None then it does it for the whole world
        spark - A spark session
        num_locales - says how many locales to return.
    """
    if country_list is None:
        data = data.drop('country').select('*', lit('All').alias('country'))
    else:
        data = data.filter(col('country').isin(country_list))
        
    data.filter((col('submission_date_s3') >= start_date) & (col('submission_date_s3') <= end_date))\
        .createOrReplaceTempView('ms')
    
    query = """
    SELECT submission_date_s3, country, locale, locale_dau / dau as locale_rate, 
            avg(locale_dau / dau) OVER(PARTITION BY country, locale 
                                  ORDER BY submission_date_s3
                                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as smoothed_locale_rate
    FROM
        (SELECT submission_date_s3, country, locale, count(distinct client_id) as locale_dau
         FROM ms
         GROUP BY submission_date_s3, country, locale) as A
    INNER JOIN
        (SELECT submission_date_s3, country, count(distinct client_id) as dau
         FROM ms
         GROUP BY submission_date_s3, country) as B
    USING(submission_date_s3, country)
    ORDER BY submission_date_s3, country
    """
    
    w = Window.partitionBy('submission_date_s3', 'country').orderBy(desc('smoothed_locale_rate'))
    return spark.sql(query).select('*', rank().over(w).alias('rank'))\
                .filter(col('rank') <= num_locales)\
                .select('submission_date_s3', 'country', 'smoothed_locale_rate')