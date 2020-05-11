import pandas as pd
from flask import jsonify
from pyspark import SQLContext
from pyspark.sql import SparkSession


class AppService:

    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Hive integration") \
            .enableHiveSupport() \
            .getOrCreate()
        self.sqlContext = SQLContext(self.spark)

    def get_uploaded_csv(self, request):
        file = request.files['file']
        data = pd.read_csv(file)
        self.spark_df = self.sqlContext.createDataFrame(data)
        return jsonify(self.spark_df.toJSON().collect())
