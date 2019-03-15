import logging
from flask import Blueprint, jsonify
import liquer
import liquer.commands

app = Blueprint('liquer', __name__, static_folder='static')
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@app.route('/', methods=['GET', 'POST'])
@app.route('/index.html', methods=['GET', 'POST'])
def index():
    return """
<html>
    <head>
        <title>LiQuer</title>
    </head>
    <body>
        <h1>LiQuer server</h1>
        For more info, see the <a href="https://github.com/orest-d/liquer-in-flask-prototype">repository</a>.
    </body>    
</html>
"""

@app.route('/q/<path:query>')
def serve(query):
    return liquer.process(query).response()

@app.route('/api/commands.json')
def commands():
    return jsonify(liquer.commands_data())

@app.route('/api/debug-json/<path:query>')
def debug_json(query):
    state = liquer.process(query).state()
    print(state)
    return jsonify(state)
