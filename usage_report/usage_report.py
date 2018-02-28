from utils.avg_daily_usage import get_daily_avg_session
from utils.avg_intensity import get_avg_intensity
from utils.pct_latest_version import pct_new_version
from utils.osdistribution import os_on_date
from utils.top10addons import top_10_addons_on_date
from utils.pct_addon import get_addon
from utils.activeuser import getMAU, getYAU
from utils.newuser import new_users
from utils.helpers import get_dest, load_main_summary, date_plus_x_days
from pyspark.sql import SparkSession
import click

# this list is formulated from
# https://sql.telemetry.mozilla.org/queries/51430/source
# may want to change
TOP_TEN_COUNTRIES = [
    'US',
    'DE',
    'FR',
    'IN',
    'BR',
    'CN',
    'ID',
    'RU',
    'IT',
    'PL'
]


def get_avg_daily_metric(f, data, **kwargs):
    return f(data,
             date=kwargs['end_date'],
             period=kwargs['lag_days'],
             country_list=kwargs['country_list'],
             locale_list=kwargs['locale_list'])


def agg_usage(spark, data, **kwargs):
    end_date = kwargs['end_date']
    country_list = kwargs['country_list']

    avg_daily_session_length = get_avg_daily_metric(get_daily_avg_session, data, **kwargs)
    avg_daily_intensity = get_avg_daily_metric(get_avg_intensity, data, **kwargs)
    pct_last_version = pct_new_version(data,
                                       date=end_date,
                                       country_list=country_list,
                                       spark=spark)

    # for mau and yau, start_date = end_date
    # since we only want ONE number for each week
    mau = getMAU(data,
                 date=end_date,
                 country_list=country_list)

    yau = getYAU(data,
                 date=end_date,
                 country_list=country_list)

    new_user_counts = new_users(data,
                                date=end_date,
                                country_list=country_list)

    os = os_on_date(data, date=end_date, country_list=country_list)
    top10addon = top_10_addons_on_date(data,
                                       date=end_date,
                                       topN=10,
                                       country_list=country_list)

    has_addon = get_addon(data, end_date, country_list)

    # to be added: os_distribution, newuser, localdistribution, active_user
    on = ['submission_date_s3', 'country']
    return avg_daily_session_length\
        .join(avg_daily_intensity, on=on)\
        .join(pct_last_version, on=on)\
        .join(mau, on=on)\
        .join(yau, on=on)\
        .join(new_user_counts, on=on)\
        .join(os, on=on)\
        .join(top10addon, on=on)\
        .join(has_addon, on=on)


@click.command()
@click.option('--date', required=True)
@click.option('--lag-days', default=7)
@click.option('--no-output', default=False, is_flag=True)
@click.option('--input-bucket', default='telemetry-parquet')
@click.option('--input-prefix', default='main_summary')
@click.option('--input-version', default='v4')
@click.option('--output-bucket', default='telemetry-parquet')
@click.option('--output-prefix', default='usage-report')  # TBD, this is a placeholder
@click.option('--output-version', default='v1')  # TBD, this is a placeholder
def main(date, lag_days, no_output, input_bucket, input_prefix, input_version,
         output_bucket, output_prefix, output_version):

    spark = (SparkSession
             .builder
             .appName("usage_report")
             .getOrCreate())

    start_date, end_date = date_plus_x_days(date, -lag_days), date

    # load main_summary with unbounded history, since YAU
    # looks at past 365 days
    ms = (
        load_main_summary(spark, input_bucket, input_prefix, input_version)
        .filter("submission_date_s3 <= '{}'".format(end_date))
        .filter("sample_id = '42'")
        .filter("normalized_channel = 'release'")
        .filter("app_name = 'Firefox'"))

    agg = agg_usage(spark, ms, start_date=start_date, end_date=end_date,
                    country_list=TOP_TEN_COUNTRIES, locale_list=None, lag_days=lag_days)
    agg.printSchema()
    # to do:
    # jsonify agg
    print "Converting data to JSON"

    if no_output:
        print "no output generated due to user request"
    else:
        # to do
        # write output to s3
        print "Writing data to", get_dest(output_bucket, output_prefix, output_version)


if __name__ == '__main__':
    main()
