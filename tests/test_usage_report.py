
import pytest

from helpers.utils import is_same
from pyspark.sql import SparkSession
from usage_report.utils.avg_intensity import getAvgIntensity
from usage_report.utils.avg_daily_usage import getDailyAvgSession
from usage_report.utils.pct_latest_version import pctnewversion
from usage_report.utils.activeuser import getMAU, getYAU

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
    return (
        (("20180201", 100, 20, "DE", "client1", "58.0.1"),
         ("20180201", 100, 20, "DE", "client1", "58.0.1")),
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version"]
    )


def test_get_avg_intensity_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getAvgIntensity(main_summary, "20180201")

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
    with_country_list = getAvgIntensity(main_summary, "20180201", country_list=["DE"])

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
    without_country_list = getDailyAvgSession(main_summary, "20180201")

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_daily_subsession_length": 200.0,
            "avg_daily_usage(hours)": 200.0 / 3600
        }
    ]

    is_same(spark, without_country_list, expected)


def test_get_avg_daily_usage_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getDailyAvgSession(main_summary, "20180201", country_list=["DE"])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "avg_daily_subsession_length": 200.0,
            "avg_daily_usage(hours)": 200.0 / 3600
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "avg_daily_subsession_length": 200.0,
            "avg_daily_usage(hours)": 200.0 / 3600
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_latest_version_no_country_list(spark, main_summary_data):
    # 'country', 'submission_date_s3', 'lastest_version_count',
    #                                     'pct_latest_version', 'is_release_date'
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pctnewversion(spark, main_summary, start_date="20180201",
                                         end_date="20180201")

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "lastest_version_count": 1,
            "pct_latest_version": 1.0,
            "is_release_date": 0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_pct_latest_version_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = pctnewversion(spark, main_summary, start_date="20180201",
                                         end_date="20180201", country_list=['DE'])

    expected = [
        {
            "country": "All",
            "submission_date_s3": "20180201",
            "lastest_version_count": 1,
            "pct_latest_version": 1.0,
            "is_release_date": 0
        },
        {
            "country": "DE",
            "submission_date_s3": "20180201",
            "lastest_version_count": 1,
            "pct_latest_version": 1.0,
            "is_release_date": 0
        }
    ]

    is_same(spark, without_country_list, expected)


def test_MAU_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getMAU(spark.sparkContext, main_summary,
                                  start_date='20180201', end_date='20180201',
                                  freq=1, factor=100, country_list=None)

    expected = [
        {
            "country": "All",
            "active_users": 100,
            "start_date": "20180104",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, without_country_list, expected)


def test_MAU_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = getMAU(spark.sparkContext, main_summary,
                               start_date='20180201', end_date='20180201',
                               freq=1, factor=100, country_list=["DE"])

    expected = [
        {
            "country": "All",
            "active_users_MAU": 100,
            "start_date_MAU": "20180104",
            "submission_date_s3": "20180201"
        },
        {
            "country": "DE",
            "active_users_MAU": 100,
            "start_date_MAU": "20180104",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, with_country_list, expected, verbose=True)


def test_YAU_no_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    without_country_list = getYAU(spark.sparkContext, main_summary,
                                  start_date='20180201', end_date='20180201',
                                  factor=100, country_list=None)

    expected = [
        {
            "country": "All",
            "active_users_MAU": 100,
            "start_date_MAU": "20170201",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, without_country_list, expected, verbose=True)


def test_YAU_country_list(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    with_country_list = getYAU(spark.sparkContext, main_summary,
                               start_date='20180201', end_date='20180201',
                               factor=100, country_list=["DE"])

    expected = [
        {
            "country": "All",
            "active_users_MAU": 100,
            "start_date": "20170201",
            "submission_date_s3": "20180201"
        },
        {
            "country": "DE",
            "active_users_MAU": 100,
            "start_date_MAU": "20170201",
            "submission_date_s3": "20180201"
        }
    ]

    is_same(spark, with_country_list, expected, verbose=True)
