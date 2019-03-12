import hxl
import flask
from flask import Response, jsonify
import io
import traceback
import pandas as pd
from urllib.request import urlopen


MIMETYPES = dict(
    json="application/json",
    txt= 'text/plain',
    html= 'text/html',
    htm= 'text/html',
    md = 'text/markdown',
    xls = 'application/vnd.ms-excel',
    xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    ods = 'application/vnd.oasis.opendocument.spreadsheet',
    tsv='text/tab-separated-values',
    csv = 'text/csv',
    msgpack = 'application/x-msgpack',
    hdf5 = 'application/x-hdf',
    h5 = 'application/x-hdf'
)

def execute(query):
    return do(query)

def parse(query):
    return [cmd.split("~") for cmd in query.split("/")]

def do(query):
    result,filename,extension = process(query)

    mimetype = MIMETYPES.get(extension, "text/plain")
    if isinstance(result, hxl.Dataset):
        if extension == "csv":
            return Response(result.gen_csv(), mimetype='text/csv')
        else:
            result = pd.read_csv(StringIO("".join(result.gen_csv())))

    if isinstance(result, pd.DataFrame):
        df = result
        if extension == "csv":
            return Response(df.to_csv(index=False), mimetype=mimetype)
        if extension == "tsv":
            return Response(df.to_csv(index=False, sep="\t"), mimetype=mimetype)
        if extension == "json":
            return Response(df.to_json(index=False, orient="table"), mimetype=mimetype)
        if extension in ("html", "htm"):
            return Response(df.to_html(index=False), mimetype=mimetype)
        if extension == "xlsx":
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            df.to_excel(writer, function)
            writer.close()
            output.seek(0)
            return send_file(output, attachment_filename=filename, as_attachment=True)
        if extension == "msgpack":
            output = BytesIO()
            df.to_msgpack(output)
            output.seek(0)
            return send_file(output, attachment_filename=filename, as_attachment=True)
        return Response(df.to_csv(index=False), mimetype=mimetype)

    if isinstance(result, str):
        if extension in TEXT_MIMETYPES:
            return Response(result, mimetype=mimetype)
    if isinstance(result,dict):
        assert (extension=="json")
        return jsonify(result)

    return result

COMMANDS = {}

def command(f):
    global COMMANDS
    COMMANDS[f.__name__] = f
    return f

def to_url(v):
    return v[1]+"://"+"/".join(v[2:])

@command
def load(v,result):
    url = to_url(v)
    return pd.read_csv(io.BytesIO(urlopen(url).read()))

@command
def load_hdx(v,result):
    url = to_url(v)
    return hxl.data(url)

def process(query):
    ql=parse(query)
    filename=None
    extension = None
    result = None
    for i,command in enumerate(ql):
        if i == len(ql)-1:
            if len(command)==1 and '.' in command[0]:
                filename = command[0]
                extension = filename.split(".")[-1]
                break
        command_name = command[0]
        if command_name in COMMANDS:
            try:
                result = COMMANDS[command_name](command,result)
            except:
                traceback.print_exc()
                result = dict(status="ERROR", message = traceback.format_exc(), index = i, command = command, query = query)
                filename = "error.json"
                extension = "json"
                return result, filename, extension
        else:
            result = dict(status="ERROR", message="Unknown command:%s"%command_name, index=i, command = command, query = query)
            filename = "error.json"
            extension = "json"
            return result, filename, extension

    return result, filename, extension
