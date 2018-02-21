from pyspark.sql import Row
from collections import OrderedDict


def convert_to_row(d):
    return Row(**OrderedDict(sorted(d.items())))


def is_same(spark, df, expected, verbose=False):
    expected_df = spark.sparkContext \
        .parallelize(expected) \
        .map(lambda r: Row(**OrderedDict(sorted(r.items())))) \
        .toDF()

    cols = sorted(df.columns)
    intersection = df.select(*cols).intersect(expected_df)
    expected_len, actual_len = expected_df.count(), intersection.count()

    if verbose:
        print "\nInput Dataframe\n"
        print df.collect()
        print "\nExpected Dataframe\n"
        print expected_df.collect()

    assert actual_len == expected_len, "Missing {} Rows".format(expected_len - actual_len)
