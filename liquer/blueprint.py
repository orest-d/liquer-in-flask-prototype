import logging
from flask import Flask, Response, Blueprint, request, redirect, url_for
import liquer

app = Blueprint('liquer', __name__)
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
    return liquer.execute(query)
