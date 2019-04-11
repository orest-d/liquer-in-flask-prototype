import traceback
from collections import namedtuple
import inspect
from liquer.state import State
from liquer.cache import NoCache


Command = namedtuple("Command",["f","name","label","short_doc", "doc","arguments"] )

def identifier_to_label(identifier):
    txt = identifier.replace("_"," ")
    txt = txt.replace(" id","ID")
    txt = dict(url="URL").get(txt,txt)
    txt = txt[0].upper()+txt[1:]
    return txt

def command_from_callable(f,command_f=None,skip_args=2):
    name = f.__name__
    doc = f.__doc__
    if doc is None:
        doc = ""
    if command_f is None:
        command_f=f
    short_doc = doc.split("\n")[0]
    arguments=[]
    sig = inspect.signature(f)
    for argname in list(sig.parameters)[skip_args:]:
        arg = dict(name=argname, label = identifier_to_label(argname))
        p = sig.parameters[argname]
        if p.default != inspect.Parameter.empty:
            arg["default"]=p.default
            arg["optional"]=True
        else:
            arg["optional"]=False
        arg["multiple"] = p.kind is inspect.Parameter.VAR_POSITIONAL
        arg["ui"] = "column" if "column" in argname else "text"
        if argname == "url":
            arg["multiple"] = False
            arg["ui"] = "url"
        arguments.append(arg)

    return Command(f=command_f, name=name, label=identifier_to_label(name), short_doc=short_doc, doc=doc, arguments=arguments)

def parse(query):
    return [[decode_token(etoken) for etoken in eqv.split("~")] for eqv in query.split("/")]

def encode_token(token):
    return (token.replace("-","--")
    .replace("https://","-H")
    .replace("http://","-h")
    .replace("://","-P")
    .replace("/","-I")
    .replace("~","-T")
    .replace(" ","-_")
    )

def decode_token(token):
    return (token
    .replace("-H","https://")
    .replace("-h","http://")
    .replace("-P","://")
    .replace("-I","/")
    .replace("-T","~")
    .replace("--","-")
    .replace("-_"," ")
    )

def encode(ql):
    return "/".join("~".join(encode_token(token) for token in qv) for qv in ql)

_state_factory = None

def set_state_factory(factory):
    global  _state_factory
    _state_factory = factory

def new_state():
    if _state_factory is None:
        return State()
    return _state_factory()

COMMANDS = {}

_cache = None
def set_cache(c=None):
    global _cache
    _cache = c

def cache():
    global _cache
    if _cache is None:
        _cache = NoCache()
    return _cache
        

def command(f):
    global COMMANDS
    COMMANDS[f.__name__] = command_from_callable(f)
    return f

class DataFrameCommand:
    def __init__(self,f):
        self.f=f
    def __call__(self,state,command,*arg):
        df = state.df()
        return state.with_df(self.f(df,*arg))

class DataFrameSource:
    def __init__(self,f):
        self.f=f
    def __call__(self,state,command,*arg):
        return state.with_df(self.f(*arg))

def df_command(f):
    global COMMANDS
    command_f = DataFrameCommand(f)
    cmd = command_from_callable(f,command_f=DataFrameCommand(f),skip_args=1)
    COMMANDS[f.__name__] = cmd
    return f

def df_source(f):
    global COMMANDS
    COMMANDS[f.__name__] = command_from_callable(f,command_f=DataFrameSource(f),skip_args=0)
    return f


def start_state(query):
    state = cache().get(query)
    if state is not None:
        state.log_info("Cached")
        return state,[]

    ql=parse(query)
    for i,qv in enumerate(ql):
        partial_commands = ql[:i+1]
        partial_query = encode(partial_commands)
        state = cache().get(partial_query)
        if state is not None:
            state.log_info("From cache: "+partial_query)
            ql = ql[i+1:]
            break
    if state is None:
        state = new_state()
    return state,ql

def process(query):
    state, ql = start_state(query)
    return process_ql_on_state(state,ql)

def process_on(state,query):
    ql=parse(query)
    return process_ql_on_state(state,ql)

def process_ql_on_state(state,ql):
    for i,qv in enumerate(ql):
        state.commands.append(qv)
        state.query = encode(state.commands)
        if i == len(ql)-1:
            if len(qv)==1 and '.' in qv[0]:
                state.with_filename(qv[0])
                break
        command_name = qv[0]
        if command_name in COMMANDS:
            try:
                state.log_command(qv,i+1)
                state = COMMANDS[command_name].f(state,*qv)
            except Exception as e:
                traceback.print_exc()
                return state.log_exception(message=str(e), traceback = traceback.format_exc())
        else:
            return state.log_error(message=f"Unknown command: {command_name}")
        cache().store(state, final_state=False)

    cache().store(state, final_state=False)
    return state


def commands_data():
    return {name:{key:value for key,value in cmd._asdict().items() if key!="f"} for name,cmd in COMMANDS.items()}
