import datetime as dt
import pyspark.sql.functions as F


def date_plus_x_days(date, x):
    '''
    '''

    new_date = dt.datetime.strptime(date, '%Y%m%d') + dt.timedelta(days=x)
    return new_date.strftime('%Y%m%d')


def keep_countries_and_all(data, country_list):
    """ Takes the main ping server and makes a country `All` and keeps only countries
        in country_list and All.

        Parameters:
            data: The main ping server.
            country_list: The list of countries to keep.
    """
    data_all = data.withColumn('country', F.lit('All'))

    if country_list is not None:
        data_countries = data.filter(F.col('country').isin(country_list))
        data_all = data_all.union(data_countries)

    return data_all


def get_dest(bucket, prefix, version, spark_provider='emr', date=None, sample_id=None):
    '''
    Stiches together an s3 or gcs destination.
    :param bucket: s3 or gcs bucket
    :param prefix: s3 or gcs prefix (within bucket)
    :param version: dataset version
    :param spark_provider: either 'emr' or 'dataproc'
    :return str ->
    s3|gs://bucket/prefix/version/submission_date_s3=[date]/sample_id=[sid]
    '''

    if spark_provider == 'dataproc':
        prefix = 'gs://'
    else:
        prefix = 's3://'

    suffix = ''
    if date is not None:
        suffix += "/submission_date_s3={}".format(date)
    if sample_id is not None:
        suffix += "/sample_id={}".format(sample_id)
    full_dest = prefix + '/'.join([bucket, prefix, version]) + suffix + '/'
    return full_dest


def load_main_summary(spark, input_bucket, input_prefix, input_version, spark_provider='emr'):
    '''
    Loads main_summary from the bucket constructed from
    input_bucket, input_prefix, input_version
    :param spark: SparkSession object
    :param input_bucket: s3 bucket (telemetry-parquet)
    :param input_prefix: s3 prefix (main_summary)
    :param input_version: dataset version (v4)
    :param spark_provider: either 'emr' or 'dataproc'
    :return SparkDF
    '''

    dest = get_dest(input_bucket, input_prefix, input_version, spark_provider)
    return (spark
            .read
            .option("mergeSchema", True)
            .parquet(dest))
