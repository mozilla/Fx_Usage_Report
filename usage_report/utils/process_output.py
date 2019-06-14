import datetime

# Note: the code makes the following assumptions otherwise
# it will throw an error by design:
#     dataframes are for a single submission_date_s3
#     historical data and new data have same countries and metrics
#     single metric per country (for single metrics)
# it will not throw error for:
#     faceted metrics, unique facets (addons have repeat names)
#     countries differ from previous (print warning)
#     metrics differ from last entry (print warning)

FXHEALTH_METRICS = ['YAU',
                    'MAU',
                    'pct_new_user',
                    'pct_latest_version',
                    'avg_daily_usage(hours)',
                    'avg_intensity']

WEBUSAGE_METRICS_1DIM = ['pct_addon',
                         'pct_TP']

WEBUSAGE_METRICS_2DIM = {'locale': ('locale', 'pct_on_locale'),
                         'top10addons': ('addon_name', 'pct_with_addon')}


def check_unique(df, metric_col):
    """ make sure df has 1 row for single value metrics
    params: df, pandas df, metric_col, str
    return: nothing, but raise error if assumptions not met
    """
    if len(df[metric_col]) != 1:
        raise ValueError('there should be 1 metric')


def check_dataframes(*dfs):
    """ check dataframes for assumptions we make for processing
    which are same country and same date
    params: dfs, at least 2 dataframes
    return: nothing, but raise error if assumptions not met
    """
    country_sets = map(lambda x: set(x['country']), dfs)
    date_sets = map(lambda x: set(x['submission_date_s3']), dfs)
    if not all([a == b for a, b
                in zip(country_sets[:-1],
                       country_sets[1:])]):
        raise ValueError('countries are different')
    if not all([a == b for a, b
                in zip(date_sets[:-1],
                       date_sets[1:])]):
        raise ValueError('dates are different')
    if len(date_sets[0]) != 1:
        raise ValueError('wrong number of dates')


def one_dim_extract(pd_df,
                    country,
                    metric_col):
    """get a metric for a country from a pandas df
    params: pd_df, pandas dataframe, (ex. usage)
    country: str
    metric_col: str
    return: a single numeric of some kind
    """
    pd_df_filtered = pd_df[pd_df['country'] == country].reset_index(drop=True)
    check_unique(pd_df_filtered, metric_col)
    return pd_df_filtered.iloc[0][metric_col]


def two_dim_extract(pd_df,
                    country,
                    facet_col,
                    metric_col):
    """ extract {facet: metric} dict from a pandas dataframe
    params: pd_df, faceted pandas dataframe, (ex, locales, top10addon)
    country, str
    facet_col, str (ex. 'locale')
    metric_col, str (ex. 'pct_on_locale')
    return:
    nested_dict, dict, keys are facets, values are metrics
    """
    nested_dict = {}
    pd_df_filtered = pd_df[pd_df['country'] == country].reset_index(drop=True)
    for i in pd_df_filtered.index:
        i_row = pd_df_filtered.iloc[i]
        nested_dict[i_row[facet_col]] = i_row[metric_col]
    return nested_dict


def fxhealth_per_day_country(usage_pd_df,
                             country):
    """ get fxhealth metrics
    params: usage_pd_df, pandas df
    country: country, str
    return: day_dict, {date: dict's date,
                       metrics:
                           {metric: value,
                            ...}}
    """
    day_dict = {}
    date = usage_pd_df['submission_date_s3'][0]
    date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

    day_dict['date'] = date
    day_dict['metrics'] = {}

    for metric in FXHEALTH_METRICS:
        day_dict['metrics'][metric] = one_dim_extract(usage_pd_df,
                                                      country,
                                                      metric)
    return day_dict


def webusage_per_day_country(usage_pd_df,
                             locales_pd_df,
                             topaddons_pd_df,
                             country):
    """ get webusage metrics
    params: usage_pd_df, pandas df, 1dim metrics
    locales_pd_df, topaddons_pd_df, pandas df, 2dim metrics
    country, str
    return: day_dict, {date: dict's date,
                       metrics:
                           {metric: value,
                            metric:
                                {facet: value,
                                 ...}, ...}}
    """
    day_dict = {}
    date = usage_pd_df['submission_date_s3'][0]
    date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

    day_dict['date'] = date
    day_dict['metrics'] = {}

    for metric in WEBUSAGE_METRICS_1DIM:
        day_dict['metrics'][metric] = one_dim_extract(usage_pd_df,
                                                      country,
                                                      metric)

    for df, metric in [(locales_pd_df, 'locale'),
                       (topaddons_pd_df, 'top10addons')]:
        day_dict['metrics'][metric] = two_dim_extract(df,
                                                      country,
                                                      WEBUSAGE_METRICS_2DIM[metric][0],
                                                      WEBUSAGE_METRICS_2DIM[metric][1])
    return day_dict


def all_metrics_per_day(country_list, usage_pd_df, locales_pd_df, topaddons_pd_df):
    """ get fxhealth and webusage metrics, all countries
    params: country_list, list of strings
    various dfs, pandas dfs
    return: tuple of dicts
    """
    check_dataframes(usage_pd_df,
                     locales_pd_df,
                     topaddons_pd_df)
    fxhealth, webusage = {}, {}
    country_list = country_list + ['All']
    for country in country_list:
        fxhealth[country] = fxhealth_per_day_country(usage_pd_df,
                                                     country)
        webusage[country] = webusage_per_day_country(usage_pd_df,
                                                     locales_pd_df,
                                                     topaddons_pd_df,
                                                     country)
    return (fxhealth, webusage)


def rename_keys(input_dict, country_name_mappings):
    """ copy dict with country keys renamed with full names
    params: input_dict, a metric dict
    country_name_mappings, dict, {abbr. country: full country}
    return: output_dict, same as input with renamed keys
    """
    return {country_name_mappings[k]: v
            for k, v in input_dict.iteritems()}


def check_dict_keys(dict1, dict2, message):
    """ check if keys are the same
    params: dict1, dict2, comparison dicts
    message, what to print
    return: nothing
    """
    if set(dict1.keys()) != set(dict2.keys()):
        print message


def update_history(day_dict, history_dict=None):
    """ updates history dict,
    also checks country, metric, and date compat
    params:
    history_dict, dict, {'country': [{data1}, {data2}, ...], ...}
    day_dict, dict,  {'country': {data}, ...}
    return: copy of history_dict updated w/new day's data
    """
    if history_dict is None:
        history_dict = {}
        for country in day_dict.keys():
            history_dict[country] = [day_dict[country]]
    history_dict = history_dict.copy()
    check_dict_keys(history_dict,
                    day_dict,
                    "warning: countries don't match")
    for country in day_dict.keys():
        if country in history_dict.keys():
            check_dict_keys(history_dict[country][-1],
                            day_dict[country],
                            "warning: metrics don't match last entry ({})".format(country))
            previous_dates = [entry['date'] for entry in history_dict[country]]
            if day_dict[country]['date'] in previous_dates:
                replace_position = previous_dates.index(day_dict[country]['date'])
                history_dict[country][replace_position] = day_dict[country]
            else:
                history_dict[country].append(day_dict[country])
        else:
            history_dict[country] = [day_dict[country]]
    return history_dict
