import os

import pandas as pd

UPLOAD_DIRECTORY = "./files"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


def write_csv(pandas_df):
    """
    csv writer for pandas df only
    """
    pandas_df.to_csv("files/input_file.csv", index=False)


def write_result_csv(spark_df):
    """
    csv writer for spark df only
    """
    spark_df.toPandas().to_csv("files/output.csv")


def write_history_csv(datetime, function, code,params="na"):
    d={}
    d['date']=datetime
    d['function']=function
    d['code']=code
    d['params']=params
    l=[d]
    df=pd.DataFrame(l)
    df.to_csv(UPLOAD_DIRECTORY+'/history.csv', mode='a', header=False)


def undo_history():
    df = pd.read_csv(UPLOAD_DIRECTORY + '/history.csv')
    if len(df) == 0:
        return 1
    elif len(df) >= 1:
        df = df.drop(df.tail(1).index)
        df.to_csv(UPLOAD_DIRECTORY + '/history.csv', index=False)


def write_history_backup(df):
    df.to_csv(UPLOAD_DIRECTORY+'/history_backup.csv', mode='a', header=False)


def clean_history():
    df=pd.read_csv(UPLOAD_DIRECTORY+'/history.csv')
    if len(df) == 0:
        return 1
    elif len(df)>=1:
        df=df.drop(df.head(1).index)
        df.to_csv(UPLOAD_DIRECTORY+'/history.csv', index=False)

def clean_all_history():
    df=pd.read_csv(UPLOAD_DIRECTORY+'/history.csv')
    if len(df) == 0:
        return 1
    elif len(df) >= 1:
        df = df.drop(df.head(len(df)).index)
        df.to_csv(UPLOAD_DIRECTORY + '/history.csv', index=False)


def write_code():
    df = pd.read_csv(UPLOAD_DIRECTORY + '/history.csv')
    functions = df['code'].to_list()

    for x in functions:
        file1 = open(UPLOAD_DIRECTORY + "/code.txt", "a")  # append mode
        file1.write("\n{}\n".format(x))
        file1.close()


def clean_code_file():
    starting_text = "from pyspark import SQLContext \nfrom pyspark.sql import SparkSession\nimport requests as r" \
                    "\nspark = SparkSession.builder.appName('Python Spark SQL Hive integration').enableHiveSupport()" \
                    ".getOrCreate() \nsqlContext = SQLContext(self.spark)\n"
    file1 = open(UPLOAD_DIRECTORY + "/code.txt", "w")  # overwrite mode
    file1.write(starting_text)
    file1.close()