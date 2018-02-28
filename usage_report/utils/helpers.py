import datetime as dt
import pyspark.sql.functions as F


def date_plus_x_days(date, x):
    '''
    '''

    new_date = dt.datetime.strptime(date, '%Y%m%d') + dt.timedelta(days=x)
    return new_date.strftime('%Y%m%d')


def keep_countries_and_all(data, country_list):
    data_all = data.withColumn('country', F.lit('All'))

    if country_list is not None:
        data_countries = data.filter(F.col('country').isin(country_list))
        data_all = data_all.union(data_countries)

    return data_all


def get_dest(output_bucket, output_prefix, output_version, date=None, sample_id=None):
    '''
    Stiches together an s3 destination.
    :param output_bucket: s3 output_bucket
    :param output_prefix: s3 output_prefix (within output_bucket)
    :param output_version: dataset output_version
    :retrn str ->
    s3://output_bucket/output_prefix/output_version/submissin_date_s3=[date]/sample_id=[sid]
    '''
    suffix = ''
    if date is not None:
        suffix += "/submission_date_s3={}".format(date)
    if sample_id is not None:
        suffix += "/sample_id={}".format(sample_id)
    full_dest = 's3://' + '/'.join([output_bucket, output_prefix, output_version]) + suffix + '/'
    return full_dest


def load_main_summary(spark, input_bucket, input_prefix, input_version):
    '''
    Loads main_summary from the bucket constructed from
    input_bucket, input_prefix, input_version
    :param spark: SparkSession object
    :param input_bucket: s3 bucket (telemetry-parquet)
    :param input_prefix: s3 prefix (main_summary)
    :param input_version: dataset version (v4)
    :return SparkDF
    '''
    dest = get_dest(input_bucket, input_prefix, input_version)
    return (spark
            .read
            .option("mergeSchema", True)
            .parquet(dest))
