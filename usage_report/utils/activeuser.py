import time
from pyspark.sql.functions import lit, col, countDistinct

# to name columns based on period
PERIOD_DESC = {
    28: "MAU",
    365: "YAU",
    7: 'WAU'
}


def getPAU(sc, data, date, period, factor, country_list):
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
        return (
            data.filter(col('submission_date_s3') > begin_str)
                .filter(col('submission_date_s3') <= date_str)
                .groupBy('country')
                .agg((factor * countDistinct('client_id')).alias(active_users_col))
                .select('*',
                        lit(begin_str).alias(start_date_col),
                        lit(date_str).alias('submission_date_s3')))

    data_all = data.drop('country').select('*', lit('All').alias('country'))
    if country_list is not None:
        data_country = data.filter(col('country').isin(country_list))
    # define column names based on period
    active_users_col = PERIOD_DESC.get(period, "other")
    start_date_col = 'start_date_' + PERIOD_DESC.get(period, "other")
    begin = date - period * 24 * 60 * 60
    begin_str = time.strftime('%Y%m%d', time.localtime(begin))
    date_str = time.strftime('%Y%m%d', time.localtime(date))

    current_count = process_data(data_all, begin_str, date_str)
    if country_list is not None:
        df_country = process_data(data_country, begin_str, date_str)
        current_count = current_count.union(df_country)

    return current_count


def getMAU(sc, data, date, freq, factor, country_list):
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
    date = int(time.mktime(time.strptime(date, '%Y%m%d')))
    return getPAU(sc, data, date, 28, factor, country_list)


def getYAU(sc, data, date, factor, country_list):
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

    date = int(time.mktime(time.strptime(date, '%Y%m%d')))
    return getPAU(sc, data, date, 365, factor, country_list)
