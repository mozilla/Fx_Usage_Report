
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


a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout', 
             foreign_install=False, is_system=False), 
 Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout', 
             foreign_install=False, is_system=True)]

a2 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout', 
         foreign_install=False, is_system=False), 
     Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout', 
         foreign_install=False, is_system=True)]

@pytest.fixture
def main_summary_data():
    return (
        (("20180201", 100, 20, "DE", "client1", "57.0.1", 17060, "Windows_NT", 10.0, a1, {0: 0, 1: 1}),
         ("20180201", 100, 20, "DE", "client1", "57.0.1", 17060, "Windows_NT", 10.0, a1, {}),
         ("20180201", 100, 20, "DE", "client2", "58.0", 17564, "Darwin", 10.0, a2), None),  # 17564 -> 20180201
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date", "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled"]
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
            "avg_daily_subsession_length": 150.0,
            "avg_daily_usage(hours)": 300.0 / 3600 / 2.0
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_get_avg_daily_usage_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = get_daily_avg_session(main_summary, "20180201", country_list=["DE"])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_daily_subsession_length": 150.0,
            "avg_daily_usage(hours)": 300.0 / 3600 / 2.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "avg_daily_subsession_length": 150.0,
            "avg_daily_usage(hours)": 300.0 / 3600 / 2.0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_latest_version_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pct_new_version(main_summary, "20180201", spark = spark)

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "latest_version_count": 1,
            "pct_latest_version": 0.5,
            "is_release_date": 0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_latest_version_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pct_new_version(main_summary, "20180201", country_list = ['DE'], spark = spark)

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "latest_version_count": 1,
            "pct_latest_version": 0.5,
            "is_release_date": 0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "latest_version_count": 1,
            "pct_latest_version": 0.5,
            "is_release_date": 0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_MAU_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getMAU(main_summary,
                                  date='20180201',
                                  country_list=None)

    expected = [
        {
            "country": "All",
            "active_users": 200,
            "start_date": "20180104",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, without_country_list, expected)


def test_MAU_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = getMAU(main_summary,
                               date='20180201',
                               country_list=["DE"])

    expected = [
        {
            "country": "All",
            "MAU": 200,
            "start_date_MAU": "20180104",
            "submission_date_s3": "20180201"
        },
        {
            "country": "DE",
            "MAU": 200,
            "start_date_MAU": "20180104",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, with_country_list, expected)


def test_YAU_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getYAU(main_summary,
                                  date='20180201', country_list=None)

    expected = [
        {
            "country": "All",
            "MAU": 200,
            "start_date_YAU": "20170201",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, without_country_list, expected)


def test_YAU_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = getYAU(main_summary,
                               date='20180201',
                               country_list=["DE"])

    expected = [
        {
            "country": "All",
            "YAU": 200,
            "start_date_YAU": "20170201",
            "submission_date_s3": "20180201"
        },
        {
            "country": "DE",
            "YAU": 200,
            "start_date_YAU": "20170201",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, with_country_list, expected)


def test_new_users_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = new_users(main_summary,
                                     date='20180201',
                                     country_list=None)

    expected = [
        {
            "country": "All",
            "submission_date_S3": "20180201",
            "new_user_rate": 0.5,
            "WAU": 200,
            "new_users": 100,
            "start_date_WAU": "20180125"
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_new_users_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = new_users(main_summary,
                                     date='20180201',
                                     country_list=["DE"])

    expected = [
        {
            "country": "All",
            "submission_date_S3": "20180201",
            "new_user_rate": 0.5,
            "WAU": 200,
            "new_users": 100,
            "start_date_WAU": "20180125"
        },
        {
            "country": "DE",
            "submission_date_S3": "20180201",
            "new_user_rate": 0.5,
            "WAU": 200,
            "new_users": 100,
            "start_date_WAU": "20180125"
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_os_distribution_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = os_on_date(main_summary,
                                     date='20180201',
                                     country_list=None)
#'country', 'start_date', 'submission_date_s3', col('nice_os').alias('os'),
#                       (col('WAU_on_OS') / col('WAU')).alias('ratio_on_os')
    expected = [
        {
            "country": "All",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "ratio_on_os" : .5
        },
        {
            "country": "All",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "ratio_on_os" : .5
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_os_distribution_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = os_on_date(main_summary,
                                     date='20180201',
                                     country_list=['DE'])
#'country', 'start_date', 'submission_date_s3', col('nice_os').alias('os'),
#                       (col('WAU_on_OS') / col('WAU')).alias('ratio_on_os')
    expected = [
        {
            "country": "All",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "ratio_on_os" : .5
        },
        {
            "country": "All",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "ratio_on_os" : .5
        },
        {
            "country": "DE",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "os": "Mac OS X",
            "ratio_on_os" : .5
        },
        {
            "country": "DE",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "os": "Windows 10",
            "ratio_on_os" : .5
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_top_10_addons_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    
    without_country_list = top_10_addons_on_date(main_summary,
                                     '20180201', 5)
    expected = [
        {
            "country": "All",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "addon_id" : u'disableSHA1rollout',
            "name" : u'SHA-1 deprecation staged rollout',
            "percent_of_active_users" : 1.0,
            "rank" : 1,
            "number_of_users" : 2,
            "wau" : 2
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)

def test_top_10_addons_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    
    without_country_list = top_10_addons_on_date(main_summary,
                                     '20180201', 5, country_list = ['DE'])
    expected = [
        {
            "country": "All",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "addon_id" : u'disableSHA1rollout',
            "name" : u'SHA-1 deprecation staged rollout',
            "percent_of_active_users" : 1.0,
            "rank" : 1,
            "number_of_users" : 2,
            "wau" : 2
        },
        {
            "country": "DE",
            "start_date" : "20180125",
            "submission_date_s3": "20180201",
            "addon_id" : u'disableSHA1rollout',
            "name" : u'SHA-1 deprecation staged rollout',
            "percent_of_active_users" : 1.0,
            "rank" : 1,
            "number_of_users" : 2,
            "wau" : 2
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)

#'submission_date_s3', 'country', 'WAU', 'add_on_count', 'pct_Addon'
def test_has_addons_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    
    without_country_list = get_addon(main_summary,
                                     '20180201')
    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "WAU" : 2,
            "add_on_count" : 2,
            "pct_Addon" : 1.0
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)

def test_has_addons_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    
    without_country_list = get_addon(main_summary,
                                     '20180201', country_list = ['DE'])
    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "WAU" : 2,
            "add_on_count" : 2,
            "pct_Addon" : 1.0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "WAU" : 2,
            "add_on_count" : 2,
            "pct_Addon" : 1.0
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_pct_tracking_protection_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = pct_tracking_protection(main_summary,
                                                '20180201',
                                                ["DE"])
    expected = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "pct_TP": 0.5
        },
        {
            "submission_date_s3": "20180201",
            "country": "DE",
            "pct_TP": 0.5
        }
    ]

    is_same(spark, with_country_list, expected, verbose=True)


def test_pct_tracking_protection_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pct_tracking_protection(main_summary,
                                                   '20180201',
                                                   None)
    expected = [
        {
            "submission_date_s3": "20180201",
            "country": "All",
            "pct_TP": 0.5
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)
