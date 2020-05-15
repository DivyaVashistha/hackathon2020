import os

import pandas as pd

UPLOAD_DIRECTORY = "./files"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)
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


def write_history_csv(datetime, function, code):
    d={}
    d['date']=datetime
    d['function']=function
    d['code']=code
    l=[d]
    df=pd.DataFrame(l)
    df.to_csv(UPLOAD_DIRECTORY+'/history.csv', mode='a', header=False)


def undo_history():
    df=pd.read_csv(UPLOAD_DIRECTORY+'/history.csv')
    if len(df) == 0:
        return 1
    elif len(df)>=1:
        df=df.drop(df.tail(1).index)
        df.to_csv(UPLOAD_DIRECTORY+'/history.csv', index=False)


def write_code():
    df = pd.read_csv(UPLOAD_DIRECTORY+'/history.csv')
    functions=df['code'].to_list()

    for x in functions:
        file1 = open(UPLOAD_DIRECTORY+"/code.txt", "a")  # append mode
        file1.write("\n{}\n".format(x))
        file1.close()
