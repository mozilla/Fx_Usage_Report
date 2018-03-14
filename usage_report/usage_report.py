from utils.avg_daily_usage import get_daily_avg_session
from utils.avg_intensity import get_avg_intensity
from utils.pct_latest_version import pct_new_version
from utils.osdistribution import os_on_date
from utils.top10addons import top_10_addons_on_date
from utils.pct_addon import get_addon
from utils.activeuser import getMAU, getYAU
from utils.newuser import new_users
from utils.localedistribution import locale_on_date
from utils.trackingprotection import pct_tracking_protection
from utils.helpers import load_main_summary
from utils.process_output import all_metrics_per_day, rename_keys, update_history
from utils.s3_utils import read_from_s3, write_to_s3
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

# country name mappings for presentation
country_name_mappings = {
    'All': 'Worldwide',
    'US': 'United States',
    'DE': 'Germany',
    'FR': 'France',
    'IN': 'India',
    'BR': 'Brazil',
    'CN': 'China',
    'ID': 'Indonesia',
    'RU': 'Russia',
    'IT': 'Italy',
    'PL': 'Poland'
}


def agg_usage(data, **kwargs):
    date = kwargs['date']
    period = kwargs['period']
    country_list = kwargs['country_list']
    sample_factor = kwargs['sample_factor']

    avg_daily_session_length = get_daily_avg_session(data,
                                                     date,
                                                     period=period,
                                                     country_list=country_list)

    avg_daily_intensity = get_avg_intensity(data,
                                            date,
                                            period=period,
                                            country_list=country_list)

    pct_last_version = pct_new_version(data,
                                       date,
                                       period=period,
                                       country_list=country_list)

    # for mau and yau, start_date = date
    # since we only want ONE number for each week
    mau = getMAU(data,
                 date,
                 sample_factor=sample_factor,
                 country_list=country_list)

    yau = getYAU(data,
                 date,
                 sample_factor=sample_factor,
                 country_list=country_list)

    new_user_counts = new_users(data,
                                date,
                                period=period,
                                country_list=country_list)

    os = os_on_date(data,
                    date,
                    period=period,
                    country_list=country_list)

    top10addon = top_10_addons_on_date(data,
                                       date,
                                       topN=10,
                                       period=period,
                                       country_list=country_list)

    has_addon = get_addon(data,
                          date,
                          period=period,
                          country_list=country_list)

    locales = locale_on_date(data,
                             date,
                             topN=5,
                             period=period,
                             country_list=country_list)

    tracking_pro = pct_tracking_protection(data,
                                           date,
                                           period=period,
                                           country_list=country_list)

    on = ['submission_date_s3', 'country']
    usage = (avg_daily_session_length
             .join(avg_daily_intensity, on=on)
             .join(pct_last_version, on=on)
             .join(mau, on=on)
             .join(yau, on=on)
             .join(new_user_counts, on=on)
             .join(has_addon, on=on)
             .join(tracking_pro, on=on))

    return usage, os, locales, top10addon


@click.command()
@click.option('--date', required=True)
@click.option('--lag-days', default=7)
@click.option('--sample', default=1, help='percent sample as int [1, 100]')
@click.option('--no-output', default=False, is_flag=True)
@click.option('--input-bucket', default='telemetry-parquet')
@click.option('--input-prefix', default='main_summary')
@click.option('--input-version', default='v4')
@click.option('--output-bucket', default='telemetry-test-bucket')
@click.option('--output-prefix', default='fxpd/testdata')  # TBD, this is a placeholder
@click.option('--output-version', default='v1')  # TBD, this is a placeholder
def main(date, lag_days, sample, no_output, input_bucket, input_prefix, input_version,
         output_bucket, output_prefix, output_version):

    spark = (SparkSession
             .builder
             .appName("usage_report")
             .getOrCreate())

    # all counts will be multipled by sample_factor
    sample_factor = 100.0 / sample

    # load main_summary with unbounded history, since YAU
    # looks at past 365 days
    ms = (
        load_main_summary(spark, input_bucket, input_prefix, input_version)
        .filter("submission_date_s3 <= '{}'".format(date))
        .filter("sample_id < {}".format(sample))
        .filter("normalized_channel = 'release'")
        .filter("app_name = 'Firefox'"))

    usage, os, locales, top10addon = agg_usage(ms, date=date, period=lag_days,
                                               sample_factor=sample_factor,
                                               country_list=TOP_TEN_COUNTRIES)
    usage.printSchema()
    usage_df = usage.toPandas()
    print usage_df

    os.printSchema()
    os_df = os.toPandas()
    print os_df

    locales.printSchema()
    locales_df = locales.toPandas()
    print locales_df

    top10addon.printSchema()
    top10addon_df = top10addon.toPandas()
    print top10addon_df

    print "Converting data to JSON"
    fxhealth, webusage = all_metrics_per_day(TOP_TEN_COUNTRIES,
                                             usage_pd_df=usage_df,
                                             os_pd_df=os_df,
                                             locales_pd_df=locales_df,
                                             topaddons_pd_df=top10addon_df)

    # rename countries for presentation
    fxhealth = rename_keys(fxhealth, country_name_mappings)
    webusage = rename_keys(webusage, country_name_mappings)
    print fxhealth
    print webusage

    # get previous data
    s3_key_fxhealth = output_prefix + '/' + output_version + '/{}/' + 'fxhealth.json'
    s3_key_webusage = output_prefix + '/' + output_version + '/{}/' + 'webusage.json'

    old_fxhealth = read_from_s3(output_bucket, s3_key_fxhealth.format('master'))
    old_webusage = read_from_s3(output_bucket, s3_key_webusage.format('master'))

    # update previous data
    fxhealth_data_full = update_history(fxhealth, old_fxhealth)
    webusage_data_full = update_history(webusage, old_webusage)

    if no_output:
        print "no output generated due to user request"
    else:
        print "Writing new data to:", output_bucket
        print s3_key_fxhealth.format('master')
        print s3_key_webusage.format('master')
        print "Writing old data to:", output_bucket
        print s3_key_fxhealth.format(date)
        print s3_key_webusage.format(date)

        # write historical data, indexed by date
        write_to_s3(output_bucket, s3_key_fxhealth.format(date), old_fxhealth)
        write_to_s3(output_bucket, s3_key_webusage.format(date), old_webusage)

        # write updated data
        write_to_s3(output_bucket, s3_key_fxhealth.format('master'), fxhealth_data_full)
        write_to_s3(output_bucket, s3_key_webusage.format('master'), webusage_data_full)


if __name__ == '__main__':
    main()
