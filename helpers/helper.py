def write_csv(pandas_df):
    """
    csv writer for pandas df only
    """
    pandas_df.to_csv("files/input_file.csv")


def write_result_csv(spark_df):
    """
    csv writer for spark df only
    """
    spark_df.toPandas().to_csv("files/output.csv")