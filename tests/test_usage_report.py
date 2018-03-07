
import pytest
from helpers.utils import is_same
from pyspark.sql import SparkSession
from usage_report.utils.avg_intensity import get_avg_intensity
from usage_report.utils.avg_daily_usage import get_daily_avg_session
from usage_report.utils.pct_latest_version import pct_new_version
from usage_report.utils.activeuser import getMAU, getYAU
from usage_report.utils.newuser import new_users
from usage_report.utils.osdistribution import os_on_date
from usage_report.utils.top10addons import top_10_addons_on_date
from usage_report.utils.pct_addon import get_addon
from usage_report.utils.localedistribution import locale_on_date
from usage_report.usage_report import agg_usage
from pyspark.sql import Row
from usage_report.utils.trackingprotection import pct_tracking_protection


#  Makes utils available
pytest.register_assert_rewrite('tests.helpers.utils')


@pytest.fixture
def spark():
    return SparkSession \
            .builder \
            .appName("usage_report_tests") \
            .getOrCreate()


@pytest.fixture
def main_summary_data():
    a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    a2 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    return (
        (("20180201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, 'en-US'),
         ("20180201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {}, "en-US"),
         ("20180201", 100, 20, "DE", "client2", "58.0", 17564,
          "Darwin", 10.0, a2, None, "DE"),  # 17564 -> 20180201
         ("20180201", 100, 20, "MX", "client3", "58.0", 17564,
          "Darwin", 10.0, a2, None, "en-US"),
         ("20180201", 100, 20, "DE", "client4", "58.0", 17554,
          "Darwin", 10.0, a2, None, "en-US"),
         ("20180131", 100, 20, "DE", "client5", "58.0", 17563,
          "Darwin", 10.0, a2, None, "DE"),
         ("20180101", 100, 20, "DE", "client5", "57.0", 17364,
          "Darwin", 10.0, a2, None, "DE"),
         ("20180101", 100, 20, "DE", "client6", "57.0", 17364,
          "Darwin", 10.0, a2, None, "DE")),
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date",
         "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled",
         "locale"]
    )


def test_get_avg_intensity_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = get_avg_intensity(main_summary, "20180201")

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_intensity": 1.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_get_avg_intensity_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = get_avg_intensity(main_summary, "20180201", country_list=["DE"])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_intensity": 1.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "avg_intensity": 1.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_get_avg_daily_usage_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = get_daily_avg_session(main_summary, "20180201")

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_daily_usage(hours)": 600.0 / 3600 / 5.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_get_avg_daily_usage_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = get_daily_avg_session(main_summary, "20180201", country_list=["DE"])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_daily_usage(hours)": 600.0 / 3600 / 5.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "avg_daily_usage(hours)": 500.0 / 3600 / 4.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_pct_latest_version_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pct_new_version(main_summary, "20180201")

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "pct_latest_version": 80.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_latest_version_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = pct_new_version(main_summary, "20180201",
                                        country_list=['DE'])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "pct_latest_version": 80.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "pct_latest_version": 75.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_MAU_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getMAU(main_summary,
                                  date='20180201',
                                  sample_factor=100.0 / 1)

    expected = [
        {
            "country": "All",
            "active_users": 500,
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_MAU_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = getMAU(main_summary,
                               date='20180201',
                               sample_factor=100.0 / 1,
                               country_list=["DE"])

    expected = [
        {
            "country": "All",
            "MAU": 500,
            "submission_date_s3": "20180201"
        },
        {
            "country": "DE",
            "MAU": 400,
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, with_country_list, expected)


def test_YAU_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getYAU(main_summary,
                                  date='20180201',
                                  sample_factor=100.0 / 1)

    expected = [
        {
            "country": "All",
            "YAU": 600,
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, without_country_list, expected)


def test_YAU_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = getYAU(main_summary,
                               date='20180201',
                               sample_factor=100.0 / 1,
                               country_list=["DE"])

    expected = [
        {
            "country": "All",
            "YAU": 600,
            "submission_date_s3": "20180201"
        },
        {
            "country": "DE",
            "YAU": 500,
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, with_country_list, expected)


def test_new_users_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = new_users(main_summary,
                                     date='20180201')

    expected = [
        {
            "country": "All",
            "submission_date_S3": "20180201",
            "pct_new_user": 60.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_new_users_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = new_users(main_summary,
                                  date='20180201',
                                  country_list=["DE"])

    expected = [
        {
            "country": "All",
            "submission_date_S3": "20180201",
            "pct_new_user": 60.0
        },
        {
            "country": "DE",
            "submission_date_S3": "20180201",
            "pct_new_user": 50.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_os_distribution_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = os_on_date(main_summary,
                                      date='20180201')

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "pct_on_os": 20.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "pct_on_os": 80.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_os_distribution_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = os_on_date(main_summary,
                                   date='20180201',
                                   country_list=['DE'])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "pct_on_os": 20.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "pct_on_os": 80.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "pct_on_os": 75.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "pct_on_os": 25.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_top_10_addons_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)

    without_country_list = top_10_addons_on_date(main_summary, '20180201', 5)
    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_top_10_addons_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)

    with_country_list = top_10_addons_on_date(main_summary, '20180201', 5, country_list=['DE'])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_has_addons_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)

    without_country_list = get_addon(main_summary, '20180201')
    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "pct_addon": 100.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_has_addons_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)

    with_country_list = get_addon(main_summary, '20180201', country_list=['DE'])
    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "pct_addon": 100.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "pct_addon": 100.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_pct_tracking_protection_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pct_tracking_protection(main_summary, '20180201')

    expected = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "pct_TP": 20.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_tracking_protection_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = pct_tracking_protection(main_summary,
                                                '20180201',
                                                country_list=["DE"])
    expected = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "pct_TP": 20.0
        },
        {
            "submission_date_s3": "20180201",
            "country": "DE",
            "pct_TP": 25.0
        }
    ]

    is_same(spark, with_country_list, expected)


def test_locale_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = locale_on_date(main_summary, '20180201', 4)
    expected = [
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

    is_same(spark, without_country_list, expected)


def test_locale_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = locale_on_date(main_summary, '20180201', 4, country_list=['DE'])

    expected = [
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

    is_same(spark, with_country_list, expected)


#
def test_integration_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, os, locales, top10addon = agg_usage(main_summary, date='20180201',
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
            "pct_new_user": 60.0,
            "pct_addon": 100.0
        }
    ]

    expected_os = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "pct_on_os": 20.0
        },
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "pct_on_os": 80.0
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
            "name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, usage, expected_usage)
    is_same(spark, os, expected_os)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)


def test_integration_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, os, locales, top10addon = agg_usage(main_summary, date='20180201',
                                               period=7, sample_factor=100.0 / 1,
                                               country_list=['DE', 'CN'])

    expected_usage = [
        {
            "submission_date_s3": "20180201",
            "country": "DE",
            "avg_daily_usage(hours)": 500.0 / 3600 / 4.0,
            "avg_intensity": 1.0,
            "pct_latest_version": 75.0,
            "pct_TP": 25.0,
            "MAU": 400,
            "YAU": 500,
            "pct_new_user": 50.0,
            "pct_addon": 100.0
        }
    ]

    expected_os = [
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "pct_on_os": 25.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "pct_on_os": 75.0
        }
    ]

    expected_locales = [
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
            "country": "DE",
            "submission_date_s3": "20180201",
            "addon_id": u'disableSHA1rollout',
            "name": u'SHA-1 deprecation staged rollout',
            "pct_with_addon": 100.0
        }
    ]

    is_same(spark, usage, expected_usage)
    is_same(spark, os, expected_os)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)
