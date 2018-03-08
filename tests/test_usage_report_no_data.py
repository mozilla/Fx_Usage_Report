import pytest
from helpers.utils import is_same
from pyspark.sql import SparkSession
from usage_report.usage_report import agg_usage
from pyspark.sql import Row


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
        (("20160201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, 'en-US'),
         ("20160101", 100, 20, "DE", "client6", "57.0", 17364,
          "Darwin", 10.0, a2, None, "DE")),
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date",
         "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled",
         "locale"]
    )


def test_integration_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, os, locales, top10addon = agg_usage(main_summary, date='20180201',
                                               period=7, sample_factor=100.0 / 1,
                                               country_list=None)

    expected_usage = []

    expected_os = []

    expected_locales = []

    expected_addons = []

    is_same(spark, usage, expected_usage)
    is_same(spark, os, expected_os)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)


def test_integration_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, os, locales, top10addon = agg_usage(main_summary, date='20180201',
                                               period=7, sample_factor=100.0 / 1,
                                               country_list=['DE', 'CN'])

    expected_usage = []

    expected_os = []

    expected_locales = []

    expected_addons = []

    is_same(spark, usage, expected_usage)
    is_same(spark, os, expected_os)
    is_same(spark, locales, expected_locales)
    is_same(spark, top10addon, expected_addons)
