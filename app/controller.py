import os

import flask
from flask import request, jsonify, Response, send_from_directory
from app.service import AppService

app = flask.Flask(__name__)
app.config["DEBUG"] = True

service = AppService()

# todo: chang this result file path
UPLOAD_DIRECTORY = "./files"
UPLOAD_DIRECTORY = os.path.abspath(UPLOAD_DIRECTORY)


@app.route("/download", methods=['GET'])
def get_file():
    """Download a file."""
    path = service.result_csv
    return send_from_directory(UPLOAD_DIRECTORY, filename=path, as_attachment=True)


# Create some test data for our catalog in the form of a list of dictionaries.
books = [
    {'id': 0,
     'title': 'A Fire Upon the Deep',
     'author': 'Vernor Vinge',
     'first_sentence': 'The coldsleep itself was dreamless.',
     'year_published': '1992'},
    {'id': 1,
     'title': 'The Ones Who Walk Away From Omelas',
     'author': 'Ursula K. Le Guin',
     'first_sentence': 'With a clamor of bells that set the swallows soaring, the Festival of Summer came to the city Omelas, bright-towered by the sea.',
     'published': '1973'},
    {'id': 2,
     'title': 'Dhalgren',
     'author': 'Samuel R. Delany',
     'first_sentence': 'to wound the autumnal city.',
     'published': '1975'}
]

@app.route('/api/v1/resources/books', methods=['GET'])
def api_id():
    # Check if an ID was provided as part of the URL.
    # If ID is provided, assign it to a variable.
    # If no ID is provided, display an error in the browser.
    if 'id' in request.args:
        id = int(request.args['id'])
    else:
        return "Error: No id field provided. Please specify an id."

    # Create an empty list for our results
    results = []

    # Loop through the data and match results that fit the requested ID.
    # IDs are unique, but other fields might return many results
    for book in books:
        if book['id'] == id:
            results.append(book)

    # Use the jsonify function from Flask to convert our list of
    # Python dictionaries to the JSON format.
    return jsonify(results)


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


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
