from flask import jsonify, Response, send_file
import pandas as pd
from io import BytesIO

class State(object):
    MIMETYPES = dict(
        json="application/json",
        txt='text/plain',
        html='text/html',
        htm='text/html',
        md='text/markdown',
        xls='application/vnd.ms-excel',
        xlsx='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        ods='application/vnd.oasis.opendocument.spreadsheet',
        tsv='text/tab-separated-values',
        csv='text/csv',
        msgpack='application/x-msgpack',
        hdf5='application/x-hdf',
        h5='application/x-hdf'
    )

    def __init__(self):
        self.query = ""
        self.sources = []
        self.columns = []
        self.column_synonyms = {}

        self.filename = None
        self.extension = None
        self.data = None
        self.log=[]
        self.is_error=False
        self.message=""

    def df(self):
        if isinstance(self.data, pd.DataFrame):
            return self.data
        raise Exception(f"Data is not a DataFrame but {type(self.data)}")
    def with_df(self,df):
        if not isinstance(df, pd.DataFrame):
            raise Exception(f"set_df: Data is not a DataFrame but {type(self.data)}")
        self.data = df
        self.with_columns(df.columns)
        return self

    def log_command(self,qv,number):
        self.log.append(dict(kind="command",qv=qv,command_number=number))
        return self
    def log_error(self,message,qv,number):
        self.log.append(dict(kind="error",qv=qv,command_number=number,message=message))
        self.is_error=True
        self.message = message
        return self
    def log_warning(self,message):
        self.log.append(dict(kind="warning",message=message))
        self.message = message
        return self
    def log_exception(self,message,traceback,qv,number):
        self.log.append(dict(kind="error",qv=qv,command_number=number,message=message,traceback=traceback))
        self.is_error=True
        self.message = message
        return self
    def log_info(self,message):
        self.log.append(dict(kind="info",message=message))
        self.message = message
        return self
    def state(self):
        return dict(
            query = self.query,
            sources = self.sources,
            columns = self.columns,
            column_synonyms = self.column_synonyms,
            filename = self.filename,
            extension = self.extension,
            mime = self.MIMETYPES.get(self.extension),
            log = self.log,
            is_error = self.is_error,
            message = self.message
        )
    def with_columns(self,columns):
        self.columns = list(columns)
        assert len(columns) == len(set(columns))
        self.column_synonyms={}
        for i,c in enumerate(columns):
            self.column_synonyms[c]=c
            for synonym in [c.lower().replace("-","_").replace(" ","_"), str(i+1)]:
                if synonym not in columns and synonym not in self.column_synonyms:
                    self.column_synonyms[synonym]=c

    def add_source(self,source):
        if source not in self.sources:
            self.sources.append(source)
        return self
    def with_filename(self,filename):
        self.filename = filename
        if "." in filename:
            self.extension = filename.split(".")[-1].lower()
        return self
    def url_source(self,qv):
        if qv[0] in ("http","https","ftp","file"):
            url = qv[0]+"://"
            if qv[0]=="file":
                url+="/"
            url+="/".join(qv[1:])
        else:
            url="http://"
            url += "/".join(qv)
        if "." in qv[-1]:
            self.with_filename(qv[-1])
        self.add_source(url)
        return url

    def expand_columns(self,columns):
        expanded = []
        for c in columns:
            cc = self.column_synonyms.get(c)
            if cc is None:
                self.log_warning(f"Column '{c}' not recognized")
            else:
                expanded.append(cc)
        return expanded

    def expand_column_values(self,column_values):
        columns = column_values[::2]
        values = column_values[1::2]
        if len(columns)<len(values):
            self.log_warning(f"Columns less than values")
        if len(columns)>len(values):
            self.log_warning(f"Columns more than values")

        expanded = []
        for c,v in zip(columns,values):
            cc = self.column_synonyms.get(c)
            if cc is None:
                self.log_warning(f"Column '{c}' not recognized")
            else:
                expanded.append((cc,v))
        return expanded

    def response(self):
        if self.is_error:
            return jsonify(self.state())

        mimetype = self.MIMETYPES.get(self.extension, "text/plain")
        result = self.data

        if isinstance(result, pd.DataFrame):
            df = result
            extension = self.extension
            filename = self.filename
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
                df.to_excel(writer)
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
            return Response(result, mimetype=mimetype)
        if isinstance(result,dict):
            assert (extension=="json")
            return jsonify(result)

        return result
