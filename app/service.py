import os
from helpers.helper import *
import pandas as pd
from flask import jsonify
from pyspark import SQLContext
from pyspark.sql import SparkSession
import requests as r
from hdfs import InsecureClient

UPLOAD_DIRECTORY = "./files"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


class AppService:

    def __init__(self):
        # clearing the content of output & input file
        df = pd.DataFrame(list())
        df.to_csv('files/output.csv')
        df.to_csv('files/input_file.csv')

        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Hive integration") \
            .enableHiveSupport() \
            .getOrCreate()
        self.sqlContext = SQLContext(self.spark)
        self.client = InsecureClient('http://localhost:9870')
        self.result_csv = 'output.csv'
        self.table_list = ''
        self.spark_df = ''

    def __del__(self):
        # clearing the content of output & input file
        df = pd.DataFrame(list())
        df.to_csv('files/output.csv')
        df.to_csv('files/input_file.csv')

    def get_uploaded_csv(self, request):
        try:
            file = request.files['file']
            data = pd.read_csv(file)
            write_csv(data)
            self.spark_df = self.sqlContext.createDataFrame(data)
            return jsonify(self.spark_df.toJSON().collect())
        except Exception as e:
            print(e)
            return None

    def get_web_csv(self, request_url):
        try:
            response = r.get(request_url).content
            df_list = pd.read_html(response)
            print(df_list)
            self.table_list = df_list
            df_list = [x.to_json() for x in df_list]
            return jsonify(df_list)
        except Exception as e:
            print(e)
            return None

    def select_web_table(self, index):
        try:
            write_csv(self.table_list[int(index)])
            self.spark_df = self.sqlContext.createDataFrame(self.table_list[int(index)])
            return jsonify(self.spark_df.toJSON().collect())
        except Exception as e:
            print(e)
            return None

    # def hdfs_makedir(self):
    #     self.client.makedirs('/hackathon')
    #
    # def hdfs_upload(self):
    #     self.client.upload(
    #         hdfs_path='/hackathon',
    #         # todo: change this filename
    #         local_path='files/daily_rides_data.csv'
    #     )
    # def hdfs_read(self):
    #     """
    #     read a csv from hdfs and store in pandas df.
    #     """
    #     with self.client.read('/tmp/my_file.csv') as reader:
    #         self.pd_df = pd.read_csv(reader, error_bad_lines=False)
    #         print(self.pd_df.head(5))
    #
    # def hdfs_write(self):
    #     """
    #     write from dataframe to csv and store in hdfs.
    #     """
    #     with self.client.write('/tmp/my_file.csv', encoding='utf-8') as writer:
    #         self.pd_df.to_csv(writer)
    #         print('done')





