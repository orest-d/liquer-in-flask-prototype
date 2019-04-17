# Example/test service to start a blueprint with custom proxies

import logging
from flask import Flask, Response
import liquer.blueprint as bp
import webbrowser
import liquer.charts
from liquer.cache import FileCache
import liquer
from liquer.state import State

app = Flask(__name__)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.INFO)
url_prefix='/liquer'
app.register_blueprint(bp.app, url_prefix=url_prefix)
#liquer.set_cache(FileCache("cache"))

def state_factory():
    global url_prefix
    state = State()
    state.vars["server"]="http://localhost:5000"
    state.vars["api_path"]=url_prefix+"/q/"

    return state

liquer.set_state_factory(state_factory)

if __name__ == '__main__':
    webbrowser.open("http://localhost:5000"+url_prefix)
    app.run(debug=True,threaded=False)
