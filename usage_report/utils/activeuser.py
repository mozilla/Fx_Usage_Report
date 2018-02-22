import time
from pyspark.sql.functions import lit, col, countDistinct

# to disguist active_users and start_date columns
PERIOD_DESC = {
    28: "MAU",
    365: "YAU",
    7: 'WAU'
}


def getPAU(sc, data, epoch_times, period, factor, country_list):
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
        and the number of unique clients_ids between start_date and submission_date_s3.
    """
    def process_data(data, begin_str, date_str):
        return data.filter(
            col('submission_date_s3') > begin_str) .filter(
            col('submission_date_s3') <= date_str) .groupBy('country') .agg(
            (factor *
             countDistinct('client_id')).alias(active_users_col)) .select(
                lit(begin_str).alias(start_date_col),
                lit(date_str).alias('submission_date_s3'),
            '*')

    res = []
    data_all = data.drop('country').select('*', lit('All').alias('country'))
    if country_list is not None:
        data_country = data.filter(col('country').isin(country_list))
    # define column names based on period
    active_users_col = 'active_users_' + PERIOD_DESC.get(period, "other")
    start_date_col = 'start_date_' + PERIOD_DESC.get(period, "other")
    for date in epoch_times:
        begin = date - period * 24 * 60 * 60
        begin_str = time.strftime('%Y%m%d', time.localtime(begin))
        date_str = time.strftime('%Y%m%d', time.localtime(date))

        current_count = process_data(data_all, begin_str, date_str)
        if country_list is not None:
            df_country = process_data(data_country, begin_str, date_str)
            current_count = current_count.union(df_country)
        res = res + [current_count]
    return sc.union([df.rdd for df in res]).toDF()


def getMAU(sc, data, start_date, end_date, freq, factor, country_list):
    """ Calculates the MAU for dates between start_date and end_date.

        Parameters:

        data - This should be a sample of the main server ping data frame.
        start_date - The first day to start calculating the MAU for.
        end_date - The last day to start calculating the MAU for.
        freq - Find the MAU every 'freq' days.
        factor - 100 / percent sample
        country_list - A list of countries that we want to calculate the
                       MAU for.
        sc - Spark context

        Output:

        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids start_date and submission_date_s3.
    """
    begin = int(time.mktime(time.strptime(start_date, '%Y%m%d')))
    end = int(time.mktime(time.strptime(end_date, '%Y%m%d')))
    # add 1 to end since range is right exclusive
    epoch_times = range(begin, end + 1, freq * 24 * 60 * 60)
    return getPAU(sc, data, epoch_times, 28, factor, country_list)


def getYAU(sc, data, start_date, end_date, factor, country_list):
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
        sc - Spark context

        Output:

        A data frame, this data frame has 3 coloumns the submission_date_s3, start_date
        and the number of unique clients_ids start_date and submission_date_s3.
    """

    begin = int(time.mktime(time.strptime(start_date, '%Y%m%d')))
    end = int(time.mktime(time.strptime(end_date, '%Y%m%d')))
    # add 1 to end since range is right exclusive
    epoch_times = range(begin, end + 1, 7 * 24 * 60 * 60)
    return getPAU(sc, data, epoch_times, 365, factor, country_list)
