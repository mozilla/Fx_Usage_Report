from pyspark.sql.functions import lit, col, countDistinct
from helpers import date_plus_x_days

# to name columns based on period
PERIOD_DESC = {
    28: "MAU",
    365: "YAU",
    7: 'WAU'
}


def getPAU(data, date, period, sample_factor=1, country_list=None):
    """ Calculates the PAU for a given period for each time in epoch_times.

        This function is fast for finding the PAU for a small number of dates.

        Paramaters:

        data - This should be a sample of the main server ping data frame.
        date - The day to calulate PAU for. This is given in epoch time.
        period - The number of days that we count distinct number of users.
                 For example MAU has a period = 28 and YAU has a period = 365.
        sample_factor - the factor to multiply counts by, pre-calculated based
                        on sample
        country_list - A list of countries that we want to calculate the
                       PAU for.

        Output:

        A data frame, this data frame has 3 columns
            submission_date_s3, country, PAU(WAU/MAU/YAU).
    """
    def process_data(data, begin, date):
        return (
            data.filter(col('submission_date_s3') > begin)
                .filter(col('submission_date_s3') <= date)
                .groupBy('country')
                .agg((sample_factor * countDistinct('client_id')).alias(active_users_col))
                .select('*',
                        lit(begin).alias(start_date_col),
                        lit(date).alias('submission_date_s3')))

    data_all = data.drop('country').select('*', lit('All').alias('country'))
    if country_list is not None:
        data_country = data.filter(col('country').isin(country_list))
    # define column names based on period
    active_users_col = PERIOD_DESC.get(period, "other")
    start_date_col = 'start_date_' + PERIOD_DESC.get(period, "other")

    begin = date_plus_x_days(date, -period)

    current_count = process_data(data_all, begin, date)
    if country_list is not None:
        df_country = process_data(data_country, begin, date)
        current_count = current_count.union(df_country)

    return current_count.select('submission_date_s3', 'country', active_users_col)


def getMAU(data, date, sample_factor=1, country_list=None):
    """ Helper function for getPAU with period 28.
    """
    return getPAU(data, date, 28, sample_factor, country_list)


def getYAU(data, date, sample_factor=1, country_list=None):
    """ Helper function for getPAU with period 365.
    """
    return getPAU(data, date, 365, sample_factor, country_list)
