import logging
from flask import Blueprint, jsonify, redirect
import liquer
import liquer.commands

app = Blueprint('liquer', __name__, static_folder='static')
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@app.route('/', methods=['GET', 'POST'])
@app.route('/index.html', methods=['GET', 'POST'])
def index():
    return redirect("/liquer/static/index.html")

@app.route('/info.html', methods=['GET', 'POST'])
def info():
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
    state = liquer.process(query)
    state_json = state.state()
    state_json["url"]=liquer.commands.Link(state,"Link","url",removelast=False).data
    return jsonify(state_json)
