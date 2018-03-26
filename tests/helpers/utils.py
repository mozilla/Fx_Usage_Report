from pyspark.sql import Row
from collections import OrderedDict


def is_same(spark, df, expected, verbose=False):
    expected_df = spark.sparkContext \
        .parallelize(expected) \
        .map(lambda r: Row(**OrderedDict(sorted(r.items())))) \
        .toDF()

    cols = sorted(df.columns)
    intersection = df.select(*cols).intersect(expected_df)
    df_len, expected_len, actual_len = df.count(), expected_df.count(), intersection.count()

    if verbose:
        print "\nInput Dataframe\n"
        print df.select(*cols).collect()
        print "\nExpected Dataframe\n"
        print expected_df.collect()

    assert df_len == expected_len
    assert actual_len == expected_len, "Missing {} Rows".format(expected_len - actual_len)
