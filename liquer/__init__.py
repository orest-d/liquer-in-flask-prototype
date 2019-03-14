import traceback
from collections import namedtuple
import inspect
from liquer.state import State

Command = namedtuple("Command",["f","name","label","short_doc", "doc","arguments"] )

def identifier_to_label(identifier):
    txt = identifier.replace("_"," ")
    txt = txt.replace(" id","ID")
    txt = dict(url="URL").get(txt,txt)
    txt = txt[0].upper()+txt[1:]
    return txt

def command_from_callable(f):
    name = f.__name__
    doc = f.__doc__
    if doc is None:
        doc = ""
    short_doc = doc.split("\n")[0]
    arguments=[]
    sig = inspect.signature(f)
    for argname in list(sig.parameters)[1:]:
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

    return Command(f=f, name=name, label=identifier_to_label(name), short_doc=short_doc, doc=doc, arguments=arguments)

def parse(query):
    query=query.replace("+"," ")
    return [cmd.split("~") for cmd in query.split("/")]


_state_factory = None

def set_state_factory(factory):
    global  _state_factory
    _state_factory = factory

def new_state():
    if _state_factory is None:
        return State()
    return _state_factory()

COMMANDS = {}

def command(f):
    global COMMANDS
    COMMANDS[f.__name__] = command_from_callable(f)
    return f

def process(query):
    ql=parse(query)
    state = new_state()
    state.query=query
    for i,qv in enumerate(ql):
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
                return state.log_exception(message=str(e), traceback = traceback.format_exc(), number = i+1, qv = qv)
        else:
            return state.log_error(message=f"Unknown command: {command_name}", number=i + 1, qv=qv)

    return state

def commands_data():
    return {name:{key:value for key,value in cmd._asdict().items() if key!="f"} for name,cmd in COMMANDS.items()}
