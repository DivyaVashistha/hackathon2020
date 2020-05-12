import flask
from flask import request, jsonify, Response
from app.service import AppService

app = flask.Flask(__name__)
app.config["DEBUG"] = True

service = AppService()
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


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Distant Reading Archive</h1>
<p>A prototype API for distant reading of science fiction novels.</p>'''


@app.route('/api/v1/resources/books/all', methods=['GET'])
def api_all():
    # jsonify, convert dict to json
    return jsonify(books)


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

    # todo: is reuqest ko is url k "{
    #     "url":"https://api.thevirustracker.com/free-api?countryTimeline=IN"
    #       }" sath hit karna to df bada ajeeb aa rha hai kyuki isme actual data thoda depth me ja kr milega
    #       means ek do property access k baad
@app.route('/transform/api', methods=["POST"])
def transform_view_api():
    data = request.json
    result = service.get_api_csv(data['url'])
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


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
