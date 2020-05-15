import os
from datetime import datetime
import flask
from flask import request, Response, send_from_directory
from flask_cors import CORS
from app.service import AppService
from helpers import helper

app = flask.Flask(__name__)
app.config["DEBUG"] = True
# enabling cors for all
CORS(app)

service = AppService()

# todo: chang this result file path
UPLOAD_DIRECTORY = "./files"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


@app.route("/download", methods=['GET'])
def get_file():
    """Download a file."""
    path = service.result_csv
    return send_from_directory(UPLOAD_DIRECTORY, filename=path, as_attachment=True)


@app.route("/get_table", methods=['GET'])
def get_table():
    result = service.get_json_df_response()
    if result:
        return result
    else:
        return Response("{'error':'csv is not uploaded yet'}", status=500, mimetype='application/json')


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@app.route('/transform', methods=["POST"])
def transform_view_csv():
    result = service.get_uploaded_csv(request)
    if result:
        return result
    else:
        return Response("{'error':'unable to load csv'}", status=500, mimetype='application/json')


@app.route('/transform/web_tables', methods=["POST"])
def transform_view_web_tables():
    data = request.json
    result = service.get_web_csv(data['url'])
    if result:
        return result
    else:
        return Response("{'error':'unable to load csv'}", status=500, mimetype='application/json')


@app.route('/choose_table/<index>')
def choose_web_table(index):
    result = service.select_web_table(index)
    if result:
        return result
    else:
        return Response("{'error':'unable to load csv'}", status=500, mimetype='application/json')


@app.route('/function/col_min/<column>', methods=['GET'])
def find_col_min(column):
    result = service.get_col_min(column)
    if result:
        helper.write_history_csv(datetime.now(), "get_col_min",
                                 'print(spark_df.agg({{{name}: "min"}}))'.format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/col_max/<column>', methods=['GET'])
def find_col_max(column):
    result = service.get_col_max(column)
    if result:
        helper.write_history_csv(datetime.now(), "get_col_max",
                                 'print(spark_df.agg({{{name}: "max"}}))'.format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/col_avg/<column>', methods=['GET'])
def find_col_avg(column):
    result = service.get_col_avg(column)
    if result:
        helper.write_history_csv(datetime.now(), "get_col_avg",
                                 'print(spark_df.agg({{{name}: "avg"}}))'.format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/col_sum/<column>', methods=['GET'])
def find_col_sum(column):
    result = service.get_col_sum(column)
    if result:
        helper.write_history_csv(datetime.now(), "get_col_sum",
                                 'print(spark_df.agg({{{name}: "sum"}}))'.format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/col_countdistinct/<column>', methods=['GET'])
def find_col_countdistinct(column):
    result = service.get_col_countdistinct(column)
    if result:
        helper.write_history_csv(datetime.now(), "get_col_countdistinct",
                                 'print(spark_df.agg(f.countDistinct(column)))')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/schema', methods=['GET'])
def get_schema():
    # service.read_original_file()
    result = service.df_printSchema()
    if result:
        helper.write_history_csv(datetime.now(), "df_printSchema", 'print(spark_df.printSchema())')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/sort/<column>/<condition>', methods=['GET'])
def sort_col(column, condition):
    result = service.order_col(column, condition)
    if result:
        helper.write_history_csv(datetime.now(), "order_col",
                                 'spark_df=spark_df.orderBy(column, ascending=bool_condition)')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/first', methods=['GET'])
def get_first():
    # service.read_original_file()
    result = service.get_first()
    if result:
        helper.write_history_csv(datetime.now(), "get_first", 'print(spark_df.first())')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/rename/<old_column_name>/<new_col_name>', methods=['GET'])
def rename_col(old_column_name, new_col_name):
    result = service.rename_column(old_column_name, new_col_name)
    if result:
        helper.write_history_csv(datetime.now(), "rename_column",
                                 'spark_df=spark_df.withColumnRenamed(old_column, new_column)')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/last', methods=['GET'])
def get_last():
    # service.read_original_file()
    result = service.get_last()
    if result:
        helper.write_history_csv(datetime.now(), "get_last",
                                 'print(spark_df.orderBy(spark_df[0],ascending=False).head(1))')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/head/<num>', methods=['GET'])
def get_head(num):
    # service.read_original_file()
    result = service.get_head(num)
    if result:
        helper.write_history_csv(datetime.now(), "get_head", 'print(spark_df.head(int(num)))')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/tail/<num>', methods=['GET'])
def get_tail(num):
    # service.read_original_file()
    result = service.get_tail(num)
    if result:
        helper.write_history_csv(datetime.now(), "get_tail",
                                 'print(spark_df.orderBy(spark_df[0],ascending=False).head(int(num)))')
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/drop/<column>', methods=['GET'])
def drop_col(column):
    result = service.drop_column(column)
    if result:
        helper.write_history_csv(datetime.now(), "drop_column",
                                 'spark_df=spark_df.drop({name})'.format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/trim/<column>', methods=['GET'])
def trim_col(column):
    result = service.trim_column(column)
    if result:
        helper.write_history_csv(datetime.now(), "trim_column",
                                 "spark_df = spark_df.withColumn('temp', f.trim(f.col({{column}})))."
                                 "drop({{column}}).withColumnRenamed('temp', {{column}})".format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/to_upper/<column>', methods=['GET'])
def upper_col(column):
    result = service.upper_column(column)
    if result:
        helper.write_history_csv(datetime.now(), "drop_column",
                                 "spark_df = spark_df.withColumn('temp', f.upper(f.col({{column}})))."
                                 "drop({{column}}).withColumnRenamed('temp', {{column}})".format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


@app.route('/function/to_lower/<column>', methods=['GET'])
def lower_col(column):
    result = service.lower_column(column)
    if result:
        helper.write_history_csv(datetime.now(), "drop_column",
                                 "spark_df = spark_df.withColumn('temp', f.lower(f.col({{column}})))."
                                 "drop({{column}}).withColumnRenamed('temp', {{column}})".format(name=column))
        return result
    else:
        return Response("{'error':'invalid operation '}", status=500, mimetype='application/json')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
