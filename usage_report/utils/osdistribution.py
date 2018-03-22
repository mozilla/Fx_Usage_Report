# deprecated for now to avoid overlap with FHR os
from pyspark.sql.functions import col, countDistinct, lit, when
from helpers import date_plus_x_days, keep_countries_and_all


def window_version(os_version):
    """
      Takes the Windows Kernel version number and
      produces the associated consumer windows version.
    """
    return when(os_version == '10.0', 'Windows 10')\
        .when(os_version == '6.1', 'Windows 7')\
        .when(os_version == '6.2', 'Windows 8')\
        .when(os_version == '6.3', 'Windows 8')\
        .when(os_version == '5.1', 'Windows XP')\
        .when(os_version == '5.2', 'Windows XP')\
        .when(os_version == '6.0', 'Windows Vista')\
        .otherwise('Other Windows')


def nice_os(os, os_version):
    """ Splits the major windows versions up and keeps mac os x and linux combined."""
    return when(os == 'Windows_NT', window_version(os_version))\
        .when(os == 'Windows_95', 'Other Windows')\
        .when(os == 'Windows_98', 'Other Windows')\
        .when(os == "Darwin", "Mac OS X")\
        .otherwise('Other')


def os_on_date(data, date,  period=7, country_list=None):
    """ Gets the distribution of OS usage calculated on the WAU on 1 day.

        Parameters:
        data: Usually the main summary data frame.
        date: day to get the os distribution for the past week.
        period: The number of days to calculate the distibution. By default it finds os
                distribution over a week.
        country_list: The countries to do the analysis. If None then it does it for the whole
                      world.

        Returns:
            submission_date_s3, country, os, pct_on_os
       """

    data_all = keep_countries_and_all(data, country_list)
    begin = date_plus_x_days(date, -period)
    data_all = data_all.select('client_id', 'submission_date_s3', 'country',
                               nice_os(col('os'), col('os_version')).alias('nice_os'))

    # Calculate the WAU
    wau = data_all\
        .filter((col('submission_date_s3') <= date) & (col('submission_date_s3') > begin))\
        .groupBy('country')\
        .agg(countDistinct('client_id').alias('WAU'))\

    os_wau = data_all\
        .filter((col('submission_date_s3') <= date) &
                (col('submission_date_s3') > begin))\
        .groupBy('country', 'nice_os')\
        .agg(countDistinct('client_id').alias('WAU_on_OS'))\
        .select(lit(begin).alias('start_date'), lit(date).alias('submission_date_s3'),
                'country', 'WAU_on_OS', 'nice_os')

    res = os_wau.join(wau, 'country', how='left')\
                .select('start_date', 'submission_date_s3',
                        'country', 'WAU_on_OS', 'nice_os', 'WAU')

    return res.select('submission_date_s3', 'country', col('nice_os').alias('os'),
                      (100.0 * col('WAU_on_OS') / col('WAU')).alias('pct_on_os'))
