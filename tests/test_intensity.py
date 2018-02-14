
import pytest

from helpers.utils import is_same
from pyspark.sql import SparkSession
from usage_report.avg_intensity import getAvgIntensity

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
        (("20180201", 100, 20, "DE", "client1"),
         ("20180201", 100, 20, "DE", "client1")),
        ["submission_date_s3", "subsession_length", "active_ticks", "country", "client_id"]
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
