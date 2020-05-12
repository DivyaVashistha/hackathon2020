import json

import pandas as pd
from flask import jsonify
from pyspark import SQLContext
from pyspark.sql import SparkSession
import requests as r

class AppService:

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Hive integration") \
            .enableHiveSupport() \
            .getOrCreate()
        self.sqlContext = SQLContext(self.spark)

    def get_uploaded_csv(self, request):
        try:
            file = request.files['file']
            data = pd.read_csv(file)
            self.spark_df = self.sqlContext.createDataFrame(data)
            return jsonify(self.spark_df.toJSON().collect())
        except Exception as e:
            print(e)
            return None

    def get_api_csv(self, request_url):
        try:
            response = r.get(request_url).json()
            result = self.spark.createDataFrame(pd.DataFrame(response))
            result.show()
            return response
        except Exception as e:
            print(e)
            return None

    # todo: convert to pyspark df.
    def get_web_csv(self, request_url):
        try:
            response = r.get(request_url).content
            df_list = pd.read_html(response)
            print(df_list)
            df_list = [x.to_json() for x in df_list]
            return jsonify(df_list)
        except Exception as e:
            print(e)
            return None