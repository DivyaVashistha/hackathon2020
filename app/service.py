import json

import pandas as pd
import pyspark.sql.functions as f
import requests as r
from flask import jsonify
from hdfs import InsecureClient
from pyspark import SQLContext
from pyspark.sql import SparkSession

from helpers.helper import *

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
            return self.get_json_df_response()
        except Exception as e:
            print(e)
            return None

    def get_json_df_response(self, number=10):
        return jsonify(self.spark_df.limit(number).toPandas().to_dict('records'))

    def get_web_csv(self, request_url):
        try:
            response = r.get(request_url).content
            df_list = pd.read_html(response)
            print(df_list)
            self.table_list = df_list
            df_list = [x.head(5).to_dict() for x in df_list]
            return jsonify(df_list)
        except Exception as e:
            print(e)
            return None

    def select_web_table(self, index):
        try:
            write_csv(self.table_list[int(index)])
            self.spark_df = self.sqlContext.createDataFrame(self.table_list[int(index)])
            return self.get_json_df_response()
        except Exception as e:
            print(e)
            return None

    def df_printSchema(self):
        try:
            schema = self.spark_df.schema.json()
            df_cols = json.loads(schema)
            l = []
            for i in df_cols['fields']:
                d = {}
                d['name'] = i['name']
                d['nullable'] = i['nullable']
                d['type'] = i['type']
                l.append(d)

            d = {'total_rows': self.spark_df.count()}
            l.append(d)
            d = {'total_columns': len(self.spark_df.columns)}
            l.append(d)
            x = jsonify(l)
            return x

        except:
            return None

    def get_first(self):
        try:
            return self.get_json_df_response(1)
        except Exception as e:
            print(e)
            return None

    def get_last(self):
        try:
            return jsonify(self.spark_df.orderBy(self.spark_df[0], ascending=False).limit(1).toPandas().to_dict('records'))
        except:
            return None

    def get_head(self, num):
        try:
            return self.get_json_df_response(num)
        except:
            return None

    def get_tail(self, num):
        try:
            return jsonify(self.spark_df.orderBy(self.spark_df[0], ascending=False).limit(int(num)).toPandas().to_dict('records'))
        except:
            return None

    def read_original_file(self):
        # for undo
        df = self.spark.read.format("csv").option("header", "true").load(UPLOAD_DIRECTORY + "/input_file.csv")
        self.spark_df = df

    def execute_final_df(self):
        # for undo
        df = pd.read_csv(UPLOAD_DIRECTORY + '/history.csv')
        functions = df['function'].to_list()
        self.read_original_file()
        for x in functions:
            method_name = getattr(self, x, lambda: "invalid")
            # Call the method as we return it
            method_name()

    def invalid(self):
        pass

    def get_col_min(self, column):
        try:
            return jsonify(self.spark_df.agg({column: "min"}).collect()[0])
        except Exception as e:
            print(e)
            return None

    def get_col_max(self, column):
        try:
            return jsonify(self.spark_df.agg({column: "max"}).collect()[0])
        except Exception as e:
            print(e)
            return None

    def get_col_sum(self, column):
        try:
            return jsonify(self.spark_df.agg({column: "sum"}).collect()[0])
        except Exception as e:
            print(e)
            return None

    def get_col_countdistinct(self, column):
        try:
            return jsonify(self.spark_df.agg(f.countDistinct(column)).collect()[0])
        except Exception as e:
            print(e)
            return None

    def get_col_avg(self, column):
        try:
            return jsonify(self.spark_df.agg({column: "avg"}).collect()[0])
        except Exception as e:
            print(e)
            return None

    def order_col(self, column, condition):
        try:
            bool_condition = True
            if condition == '0':
                bool_condition = False
            self.spark_df = self.spark_df.orderBy(column, ascending=bool_condition)
            return self.get_json_df_response()
        except Exception as e:
            print(e)
            return None

    def rename_column(self, old_column, new_column):
        try:
            self.spark_df = self.spark_df.withColumnRenamed(old_column, new_column)
            return self.get_json_df_response()
        except Exception as e:
            print(e)
            return None

    def drop_column(self, column):
        try:
            self.spark_df = self.spark_df.drop(column)
            return self.get_json_df_response()
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
