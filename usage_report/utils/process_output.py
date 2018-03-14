import datetime

# Note: the code mades the following assumptions otherwise
# it will throw an error by design:
#     dataframes are for a single submission_date_s3
#     historical data and new data have same countries and metrics
#     single metric per country (or set of metrics)


# helper function to validate data shape
def check_unique(df, metric_col, facet_col=None):
    """ make sure df has 1 row for single value metrics,
        unique facets for faceted metrics
    params: df, pandas df, metric_col, str, facet_col, str
    return: nothing, but raise error if assumptions not met
    """
    if facet_col is None:
        if len(df[metric_col]) != 1:
            raise ValueError('there should be 1 metric')
    else:
        if len(set(df[facet_col])) != len(df[facet_col]):
            raise ValueError('facets should be unique')


# make sure all dataframes have same country and dates
def check_dataframes(*args):
    """ check dataframes for assumptions we make for processing
    params: args, at least 2 dataframes
    return: nothing, but raise error if assumptions not met
    """
    country_sets = map(lambda x: set(x['country']), args)
    date_sets = map(lambda x: set(x['submission_date_s3']), args)
    # make sure all dfs have same countries
    first_country_set = country_sets.pop()
    for thing in country_sets:
        if first_country_set != thing:
            raise ValueError('countries are different')
    # make sure all dfs have same/only 1 submission_date_s3
    first_date_set = date_sets.pop()
    for thing in date_sets:
        if first_date_set != thing:
            raise ValueError('dates are different')
    if len(first_date_set) != 1:
        raise ValueError('wrong number of dates')


# helper functions for processing df
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
    # make sure only 1 value for that metric
    check_unique(pd_df_filtered, metric_col)
    return pd_df_filtered.iloc[0][metric_col]


def two_dim_extract(pd_df,
                    country,
                    metric_col,
                    facet_col,
                    check=False):
    """ extract {facet: metric} dict from a pandas dataframe
    params: pd_df, faceted pandas dataframe, (ex, os, locales, top10addon)
    country, str
    facet_col, str (ex. 'locale')
    metric_col, str (ex. 'pct_on_locale')
    return:
    nested_dict, dict, keys are facets, values are metrics
    """
    nested_dict = {}
    pd_df_filtered = pd_df[pd_df['country'] == country].reset_index(drop=True)
    # make sure facets are unique
    if check is True:
        check_unique(pd_df_filtered, metric_col, facet_col)
    for i in pd_df_filtered.index:
        i_row = pd_df_filtered.iloc[i]
        nested_dict[i_row[facet_col]] = i_row[metric_col]
    return nested_dict


# functions to get fxhealth dict and webusage per day per country
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
    # to do, check input df to make sure it fulfills conditions
    day_dict = {}
    # convert convert date format for presentation
    date = usage_pd_df['submission_date_s3'][0]
    date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

    day_dict['date'] = date
    day_dict['metrics'] = {}

    fxhealth_metrics = ['YAU',
                        'MAU',
                        'pct_new_user',
                        'pct_latest_version',
                        'avg_daily_usage(hours)',
                        'avg_intensity']

    for metric in fxhealth_metrics:
        day_dict['metrics'][metric] = one_dim_extract(usage_pd_df,
                                                      country,
                                                      metric)
    return day_dict


def webusage_per_day_country(usage_pd_df,
                             os_pd_df,
                             locales_pd_df,
                             topaddons_pd_df,
                             country):
    """ get webusage metrics
    params: usage_pd_df, pandas df, 1dim metrics
    os_pd_df, locales_pd_df, topaddons_pd_df, pandas df, 2dim metrics
    country, str
    return: day_dict, {date: dict's date,
                       metrics:
                           {metric: value,
                            metric:
                                {facet: value,
                                 ...}, ...}}
    """
    # to do, check input dfs to make sure it fulfills conditions
    day_dict = {}
    # convert convert date format for presentation
    date = usage_pd_df['submission_date_s3'][0]
    date = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')

    day_dict['date'] = date
    day_dict['metrics'] = {}

    # get single dim metrics
    webusage_metrics_1dim = ['pct_addon',
                             'pct_TP']

    for metric in webusage_metrics_1dim:
        day_dict['metrics'][metric] = one_dim_extract(usage_pd_df,
                                                      country,
                                                      metric)
    # get faceted metrics
    webusage_metrics_2dim = {'os': ('os', 'pct_on_os'),
                             'locale': ('locale', 'pct_on_locale'),
                             'top10addons': ('addon_name', 'pct_with_addon')}

    day_dict['metrics']['os'] = two_dim_extract(os_pd_df,
                                                country,
                                                webusage_metrics_2dim['os'][1],
                                                webusage_metrics_2dim['os'][0],
                                                check=True)
    day_dict['metrics']['locale'] = two_dim_extract(locales_pd_df,
                                                    country,
                                                    webusage_metrics_2dim['locale'][1],
                                                    webusage_metrics_2dim['locale'][0],
                                                    check=True)
    day_dict['metrics']['top10addons'] = two_dim_extract(topaddons_pd_df,
                                                         country,
                                                         webusage_metrics_2dim['top10addons'][1],
                                                         webusage_metrics_2dim['top10addons'][0])
    return day_dict


# get all metrics for single day
def all_metrics_per_day(country_list, **kwargs):
    """ get fxhealth and webusage metrics, all countries
    params: country_list, list of strings
    **kwargs: pandas dfs
        kw = usage_pd_df
        kw = os_pd_df
        kw = locales_pd_df
        kw = topaddons_pd_df
    return: tuple of dicts
    """
    check_dataframes(kwargs['usage_pd_df'],
                     kwargs['os_pd_df'],
                     kwargs['locales_pd_df'],
                     kwargs['topaddons_pd_df'])
    fxhealth = {}
    webusage = {}
    # add 'All' to country list
    country_list = country_list + ['All']
    for country in country_list:
        fxhealth[country] = fxhealth_per_day_country(kwargs['usage_pd_df'],
                                                     country)
        webusage[country] = webusage_per_day_country(kwargs['usage_pd_df'],
                                                     kwargs['os_pd_df'],
                                                     kwargs['locales_pd_df'],
                                                     kwargs['topaddons_pd_df'],
                                                     country)
    return (fxhealth, webusage)


# for converting country from US to United States, for presentation
def rename_keys(input_dict, country_name_mappings):
    """ copy dict with country keys renamed with full names
    params: input_dict, a metric dict
    country_name_mappings, dict, {abbr. country: full country}
    return: output_dict, same as input with renamed keys
    """
    output_dict = {}
    for country in input_dict.keys():
        output_dict[country_name_mappings[country]] = input_dict[country]
    return output_dict


# helper for making sure countries and metrics match old data
def check_dict_keys(dict1, dict2):
    """
    """
    if set(dict1.keys()) != set(dict2.keys()):
        raise ValueError('keys are different')


# add day's data to historical data
# note: day's data format is {'country': {data}, ...} but historical data
# format is {'country': [{data1}, {data2}, ...], ...}
def update_history(day_dict, history_dict=None):
    """ updates history dict,
    also checks country, metric, and date compat
    params:
    history_dict, dict, {'country': [{data1}, {data2}, ...], ...}
    day_dict, dict,  {'country': {data}, ...}
    return: copy of history_dict updated w/new day's data
    """
    # if there is no prior history, just put single day into history format
    if history_dict is None:
        history_dict = {}
        for country in day_dict.keys():
            history_dict[country] = [day_dict[country]]
    # if there is history, update it
    history_dict = history_dict.copy()
    # make sure you have same countries
    check_dict_keys(history_dict, day_dict)
    for country in history_dict.keys():
        # make sure metrics are same
        check_dict_keys(history_dict[country][-1], day_dict[country])
        # make sure you're not adding the same date, if so, do nothing
        if history_dict[country][-1]['date'] < day_dict[country]['date']:
            history_dict[country].append(day_dict[country])
    return history_dict
