from pyspark.sql import Window
from pyspark.sql.functions import col, countDistinct, lit, mean, when
import pyspark.sql.functions as F


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
        .when(os_version == '6.0', 'Windows Vista')\
        .otherwise('Other Windows')


def nice_os(os, os_version):
    """ Splits the major windows versions up and keeps mac os x and linux combined."""
    return when(os == 'Windows_NT', window_version(os_version))\
        .otherwise(os)


def os_distribution(data, start_date, end_date, country_list):
    """ Gets the distribution of OS usage calculated on the DAU,
            as well as a smoothed version (rolling 7 day mean).

        Parameters:
        data - Usually the main summary data frame
        start_date - day to start the analysis
        end_date - last day in the analysis
        country_list - the countries to do the analysis. If None then it does it for the whole world
    """
    if country_list is None:
        data = data.drop('country').select('*', lit('All').alias('country'))
    else:
        data = data.filter(col('country').isin(country_list))

    dau_by_os_df = data.filter(
        (col('submission_date_s3') > start_date) & (
            col('submission_date_s3') < end_date)) .groupBy(
        nice_os(
            col('os'),
            col('os_version')).alias('nice_os'),
        'submission_date_s3',
        'country') .agg(
        countDistinct('client_id').alias('active_users')) .cache()

    dau = dau_by_os_df.groupby('submission_date_s3', 'country')\
        .agg(F.sum('active_users').alias('dau'))

    dau_by_os_df = dau_by_os_df\
        .join(dau, on=['submission_date_s3', 'country'])\
        .select('*', (col('active_users') / col('dau')).alias('rate'))\
        .select('*',
                mean('rate')
                .over(Window.partitionBy('nice_os', 'country')
                      .orderBy('submission_date_s3')
                      .rowsBetween(-6, 0))
                .alias('smoothed_rate'))\
        .orderBy('nice_os', 'submission_date_s3')

    return dau_by_os_df.select(
        'submission_date_s3',
        'country',
        'nice_os',
        'rate',
        'smoothed_rate')
