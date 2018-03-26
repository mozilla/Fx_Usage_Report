
import pytest
from helpers.utils import is_same
from usage_report.usage_report import agg_usage, get_spark
from pyspark.sql import Row

#  Makes utils available
pytest.register_assert_rewrite('tests.helpers.utils')


@pytest.fixture
def spark():
    return get_spark()


@pytest.fixture
def main_summary_data_multiple():
    ''' data with multiple counties and days including the following cases:
          - multiple countries
            a)include countries that are not in country list
            b)include countries into country_list that are not in data
          - clients with only pings from outside date range
          - clients with some pings from outside date range
    '''
    a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    return (
        (("20180201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, 'en-US'),
         ("20180201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, "en-US"),
         ("20180201", 100, 20, "DE", "client2", "58.0", 17564,
          "Darwin", 10.0, a1, None, "DE"),  # 17564 -> 20180201
         ("20180201", 100, 20, "MX", "client3", "58.0", 17564,
          "Darwin", 10.0, a1, None, "en-US"),
         ("20180201", 100, 20, "DE", "client4", "58.0", 17554,
          "Darwin", 10.0, a1, None, "en-US"),
         ("20180131", 100, 20, "DE", "client5", "58.0", 17363,
          "Darwin", 10.0, a1, None, "DE"),
         ("20180101", 100, 20, "DE", "client5", "57.0", 17364,
          "Darwin", 10.0, a1, None, "DE"),
         ("20180101", 100, 20, "DE", "client6", "57.0", 17364,
          "Darwin", 10.0, a1, None, "DE")),
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date",
         "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled",
         "locale"]
    )


@pytest.fixture
def main_summary_data_null_value():
    ''' data with all/some of a given field are null, '', or zero
          - 'app_version' is all showing ''
          - 'profile_creation_date' has None
          - 'active_ticks' has zero
          - 'client2' has multiple fields missing
    '''
    a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    return (
        (("20180201", 100, 20, "DE", "client1", "", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, "en-US"),
         ("20180201", 100, 20, "DE", "client1", "", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, "en-US"),
         ("20180201", 100, 0, "DE", "client2", "", None,
          "Darwin", 10.0, a1, None, "DE"),  # 17564 -> 20180201
         ("20180201", 100, 20, "DE", "client4", "", 17554,
          "Darwin", 10.0, a1, None, "en-US"),
         ("20180131", 100, 20, "DE", "client5", "", 17563,
          "Darwin", 10.0, a1, None, "DE")),
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date",
         "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled",
         "locale"]
    )


def test_integration_multiple_countries_and_days_no_country_list(spark, main_summary_data_multiple):
    ''' tests without country list for data including the following cases:
          - multiple countries
            a)include countries that are not in country list
            b)include countries into country_list that are not in data
          - clients with only pings from outside date range
          - clients with some pings from outside date range
    '''
    main_summary = spark.createDataFrame(*main_summary_data_multiple)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=7, sample_factor=100.0 / 1,
                                           country_list=None)

    expected_usage = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "avg_daily_usage(hours)": 600.0 / 3600 / 5.0,
            "avg_intensity": 1.0,
            "pct_latest_version": 80.0,
            "pct_TP": 20.0,
            "MAU": 500,
            "YAU": 600,
            "pct_new_user": 40.0,
            "pct_addon": 100.0
        }
    ]

    expected_locales = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "en-US",
            "pct_on_locale": 60.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "DE",
            "pct_on_locale": 40.0
        }
    ]

    expected_addons = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "addon_name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, usage, expected_usage)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)


def test_integration_multiple_countries_and_days_country_list(spark, main_summary_data_multiple):
    ''' tests with country list for data including the following cases:
          - multiple countries
            a)include countries that are not in country list
            b)include countries into country_list that are not in data
          - clients with only pings from outside date range
          - clients with some pings from outside date range
    '''
    main_summary = spark.createDataFrame(*main_summary_data_multiple)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=7, sample_factor=100.0 / 1,
                                           country_list=['DE', 'CN'])

    expected_usage = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "avg_daily_usage(hours)": 600.0 / 3600 / 5.0,
            "avg_intensity": 1.0,
            "pct_latest_version": 80.0,
            "pct_TP": 20.0,
            "MAU": 500,
            "YAU": 600,
            "pct_new_user": 40.0,
            "pct_addon": 100.0
        },
        {
            "submission_date_s3": "20180201",
            "country": "DE",
            "avg_daily_usage(hours)": 500.0 / 3600 / 4.0,
            "avg_intensity": 1.0,
            "pct_latest_version": 75.0,
            "pct_TP": 25.0,
            "MAU": 400,
            "YAU": 500,
            "pct_new_user": 25.0,
            "pct_addon": 100.0
        },

    ]

    expected_locales = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "en-US",
            "pct_on_locale": 60.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "DE",
            "pct_on_locale": 40.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "locale": "en-US",
            "pct_on_locale": 50.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "locale": "DE",
            "pct_on_locale": 50.0
        }
    ]

    expected_addons = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "addon_name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "addon_name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, usage, expected_usage)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)


def test_integration_missing_fields_no_country_list(spark, main_summary_data_null_value):
    ''' tests without country list for data with all/some of a given field are null, '', or zero
    '''
    main_summary = spark.createDataFrame(*main_summary_data_null_value)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=7, sample_factor=100.0 / 1,
                                           country_list=None)

    expected_usage = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "avg_daily_usage(hours)": 500.0 / 3600 / 4.0,
            "avg_intensity": 0.75,
            "pct_latest_version": 0.0,
            "pct_TP": 25.0,
            "MAU": 400,
            "YAU": 400,
            "pct_new_user": 25.0,
            "pct_addon": 100.0
        }
    ]

    expected_locales = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "en-US",
            "pct_on_locale": 50.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "DE",
            "pct_on_locale": 50.0
        }
    ]

    expected_addons = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "addon_name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, usage, expected_usage)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)


def test_integration_missing_fields_country_list(spark, main_summary_data_null_value):
    ''' tests with country list for data with all/some of a given field are null, '', or zero
    '''
    main_summary = spark.createDataFrame(*main_summary_data_null_value)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=7, sample_factor=100.0 / 1,
                                           country_list=['DE'])

    expected_usage = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "avg_daily_usage(hours)": 500.0 / 3600 / 4.0,
            "avg_intensity": 0.75,
            "pct_latest_version": 0.0,
            "pct_TP": 25.0,
            "MAU": 400,
            "YAU": 400,
            "pct_new_user": 25.0,
            "pct_addon": 100.0
        },
        {
            "submission_date_s3": "20180201",
            "country": "DE",
            "avg_daily_usage(hours)": 500.0 / 3600 / 4.0,
            "avg_intensity": 0.75,
            "pct_latest_version": 0.0,
            "pct_TP": 25.0,
            "MAU": 400,
            "YAU": 400,
            "pct_new_user": 25.0,
            "pct_addon": 100.0
        }
    ]

    expected_locales = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "en-US",
            "pct_on_locale": 50.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "locale": "DE",
            "pct_on_locale": 50.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "locale": "en-US",
            "pct_on_locale": 50.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "locale": "DE",
            "pct_on_locale": 50.0
        }
    ]

    expected_addons = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "addon_name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "addon_name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, usage, expected_usage)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)
