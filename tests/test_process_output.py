import pytest
from usage_report.usage_report import agg_usage, get_spark
from pyspark.sql import Row
from usage_report.utils.process_output import all_metrics_per_day
from usage_report.utils.process_output import update_history


#  Makes utils available
pytest.register_assert_rewrite('tests.helpers.utils')


@pytest.fixture
def spark():
    return get_spark()


@pytest.fixture
def main_summary_data():
    a1 = [Row(addon_id=u'disableSHA1rollout', name=u'SHA-1 deprecation staged rollout',
              foreign_install=False, is_system=False),
          Row(addon_id=u'e10srollout@mozilla.org', name=u'Multi-process staged rollout',
              foreign_install=False, is_system=True)]

    return (
        (("20180201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {0: 0, 1: 1}, 'en-US'),
         ("20180201", 100, 20, "DE", "client1", "57.0.1", 17060,
          "Windows_NT", 10.0, a1, {}, "en-US"),
         ("20180201", 100, 20, "DE", "client2", "58.0", 17563,
          "Darwin", 10.0, a1, None, "DE")),  # 17563 -> 20180201
        ["submission_date_s3", "subsession_length", "active_ticks",
         "country", "client_id", "app_version", "profile_creation_date",
         "os", "os_version", "active_addons", "histogram_parent_tracking_protection_enabled",
         "locale"]
    )


def test_processing_one_day(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=1, sample_factor=100.0 / 1,
                                           country_list=['DE'])
    usage_df = usage.toPandas()
    locales_df = locales.toPandas()
    top10addon_df = top10addon.toPandas()

    fxhealth, webusage = all_metrics_per_day(['DE'],
                                             usage_pd_df=usage_df,
                                             locales_pd_df=locales_df,
                                             topaddons_pd_df=top10addon_df)

    expected_fxhealth = {
        'DE': {"date": "2018-02-01",
               "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                           "avg_intensity": 1.0,
                           "pct_latest_version": 50.0,
                           "MAU": 200.0,
                           "YAU": 200.0,
                           "pct_new_user": 50.0}},
        'All': {"date": "2018-02-01",
                "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                            "avg_intensity": 1.0,
                            "pct_latest_version": 50.0,
                            "MAU": 200.0,
                            "YAU": 200.0,
                            "pct_new_user": 50.0}}
                        }

    expected_webusage = {
        'DE': {"date": "2018-02-01",
               "metrics": {"pct_TP": 50.0,
                           "pct_addon": 100.0,
                           "locale": {u"en-US": 50.0,
                                      u"DE": 50.0},
                           "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}},
        'All': {"date": "2018-02-01",
                "metrics": {"pct_TP": 50.0,
                            "pct_addon": 100.0,
                            "locale": {u"en-US": 50.0,
                                       u"DE": 50.0},
                            "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}
                        }

    assert expected_fxhealth == fxhealth
    assert expected_webusage == webusage


def test_update_history_fxhealth_with_history(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=1, sample_factor=100.0 / 1,
                                           country_list=['DE'])
    usage_df = usage.toPandas()
    locales_df = locales.toPandas()
    top10addon_df = top10addon.toPandas()

    fxhealth, webusage = all_metrics_per_day(['DE'],
                                             usage_pd_df=usage_df,
                                             locales_pd_df=locales_df,
                                             topaddons_pd_df=top10addon_df)

    old_fxhealth = {
        'DE': [
                {"date": "2018-01-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}}
              ],
        'All': [
                {"date": "2018-01-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}}
               ]
                   }
    updated_fxhealth = update_history(fxhealth, old_fxhealth)

    expected_fxhealth = {
        'DE': [
                {"date": "2018-01-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}},
                {"date": "2018-02-01",
                         "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                                     "avg_intensity": 1.0,
                                     "pct_latest_version": 50.0,
                                     "MAU": 200.0,
                                     "YAU": 200.0,
                                     "pct_new_user": 50.0}}
              ],
        'All': [
                {"date": "2018-01-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}},
                {"date": "2018-02-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}}
               ]
                        }

    assert expected_fxhealth == updated_fxhealth


def test_update_history_fxhealth_without_history(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=1, sample_factor=100.0 / 1,
                                           country_list=['DE'])
    usage_df = usage.toPandas()
    locales_df = locales.toPandas()
    top10addon_df = top10addon.toPandas()

    fxhealth, webusage = all_metrics_per_day(['DE'],
                                             usage_pd_df=usage_df,
                                             locales_pd_df=locales_df,
                                             topaddons_pd_df=top10addon_df)

    updated_fxhealth = update_history(fxhealth, None)

    expected_fxhealth = {
        'DE': [
                {"date": "2018-02-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}}
              ],
        'All': [
                {"date": "2018-02-01",
                 "metrics": {"avg_daily_usage(hours)": 300.0 / 3600 / 2.0,
                             "avg_intensity": 1.0,
                             "pct_latest_version": 50.0,
                             "MAU": 200.0,
                             "YAU": 200.0,
                             "pct_new_user": 50.0}}
               ]
                   }

    assert expected_fxhealth == updated_fxhealth


def test_update_history_webusage_with_history(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=1, sample_factor=100.0 / 1,
                                           country_list=['DE'])
    usage_df = usage.toPandas()
    locales_df = locales.toPandas()
    top10addon_df = top10addon.toPandas()

    fxhealth, webusage = all_metrics_per_day(['DE'],
                                             usage_pd_df=usage_df,
                                             locales_pd_df=locales_df,
                                             topaddons_pd_df=top10addon_df)

    old_webusage = {
        'DE': [
                {"date": "2018-01-01",
                 "metrics": {"pct_TP": 50.0,
                             "pct_addon": 100.0,
                             "locale": {u"en-US": 50.0,
                                        u"DE": 50.0},
                             "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}
              ],
        'All': [
                {"date": "2018-01-01",
                 "metrics": {"pct_TP": 50.0,
                             "pct_addon": 100.0,
                             "locale": {u"en-US": 50.0,
                                        u"DE": 50.0},
                             "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}
               ]
                        }

    updated_webusage = update_history(webusage, old_webusage)

    expected_webusage = {
        'DE': [
               {"date": "2018-01-01",
                "metrics": {"pct_TP": 50.0,
                            "pct_addon": 100.0,
                            "locale": {u"en-US": 50.0,
                                       u"DE": 50.0},
                            "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}},
               {"date": "2018-02-01",
                "metrics": {"pct_TP": 50.0,
                            "pct_addon": 100.0,
                            "locale": {u"en-US": 50.0,
                                       u"DE": 50.0},
                            "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}
              ],
        'All': [
                {"date": "2018-01-01",
                 "metrics": {"pct_TP": 50.0,
                             "pct_addon": 100.0,
                             "locale": {u"en-US": 50.0,
                                        u"DE": 50.0},
                             "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}},
                {"date": "2018-02-01",
                 "metrics": {"pct_TP": 50.0,
                             "pct_addon": 100.0,
                             "locale": {u"en-US": 50.0,
                                        u"DE": 50.0},
                             "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}

               ]
                        }

    assert expected_webusage == updated_webusage


def test_update_history_webusage_without_history(spark, main_summary_data):
    main_summary = spark.createDataFrame(*main_summary_data)
    usage, locales, top10addon = agg_usage(main_summary, date='20180201',
                                           period=1, sample_factor=100.0 / 1,
                                           country_list=['DE'])
    usage_df = usage.toPandas()
    locales_df = locales.toPandas()
    top10addon_df = top10addon.toPandas()

    fxhealth, webusage = all_metrics_per_day(['DE'],
                                             usage_pd_df=usage_df,
                                             locales_pd_df=locales_df,
                                             topaddons_pd_df=top10addon_df)

    updated_webusage = update_history(webusage, None)

    expected_webusage = {
        'DE': [
                {"date": "2018-02-01",
                 "metrics": {"pct_TP": 50.0,
                             "pct_addon": 100.0,
                             "locale": {u"en-US": 50.0,
                                        u"DE": 50.0},
                             "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}
              ],
        'All': [
                {"date": "2018-02-01",
                 "metrics": {"pct_TP": 50.0,
                             "pct_addon": 100.0,
                             "locale": {u"en-US": 50.0,
                                        u"DE": 50.0},
                             "top10addons": {u'SHA-1 deprecation staged rollout': 100.0}}}
               ]
                        }

    assert expected_webusage == updated_webusage
