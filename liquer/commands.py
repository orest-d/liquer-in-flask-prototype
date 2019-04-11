from liquer import *
import pandas as pd
import io
from urllib.request import urlopen
import base64

@command
def From(state, command, *url):
    """Load data from URL
    Separate protocol as an optional first argument, replace '/' by '~' in the url.
    Example: From~https~example.com~data.csv
    """
    url = state.url_source(url)
    state.log_info(f"From URL: {url}")
    extension = state.extension
    if extension == "csv":
        df = pd.read_csv(io.BytesIO(urlopen(url).read()))
    elif extension == "tsv":
        df = pd.read_csv(io.BytesIO(urlopen(url).read()),sep="\t")
    elif extension in ("xls", "xlsx"):
        df = pd.read_excel(io.BytesIO(urlopen(url).read()))
    else:
        raise Exception(f"Unsupported file extension: {extension}")
    return state.with_df(df)

@command
def Append(state, command, *url):
    """Append data from URL
    Separate protocol as an optional first argument, replace '/' by '~' in the url.
    Example: Append~https~example.com~data.csv
    """
    url = state.url_source(url)
    state.log_info(f"Append from URL: {url}")
    extension = state.extension
    if extension == "csv":
        df = pd.read_csv(io.BytesIO(urlopen(url).read()))
    elif extension == "tsv":
        df = pd.read_csv(io.BytesIO(urlopen(url).read()),sep="\t")
    elif extension in ("xls", "xlsx"):
        df = pd.read_excel(io.BytesIO(urlopen(url).read()))
    else:
        raise Exception(f"Unsupported file extension: {extension}")
    return state.with_df(state.df().append(df,ignore_index=True))

#@command
#def Echo(state, command, *txt):
#    state.data=" ".join(txt)
#    state.extension = "txt"
#    return state

@command
def KeepCol(state, command, *columns):
    """Keep columns
    Keep only specified columns, remove everything else.
    Example: KeepCol~column1~column2
    """
    c = state.expand_columns(columns)
    state.log_info("Keep columns: "+(", ".join(c)))
    df = state.df()
    return state.with_df(df.loc[:,c])

@command
def RmCol(state, command, *columns):
    """Remove columns
    Remove specified columns.
    Example: RmCol~column1
    """
    remove = state.expand_columns(columns)
    state.log_info("Remove columns: "+(", ".join(remove)))
    keep = [c for c in state.columns if c not in remove]

    df = state.df()
    return state.with_df(df.loc[:,keep])

@command
def Eq(state, command, *column_values):
    """Equals filter
    Accepts one or more column-value pairs. Keep only rows where value in the column equals specified value.
    Example: Eq~column1~1
    """
    df = state.df()
    for c,v in state.expand_column_values(column_values):
        state.log_info(f"Equals: {c} == {v}")
        index = df[c] == v
        try:
            index = index | (df[c] == int(v))
        except:
            pass
        try:
            index = index | (df[c] == float(v))
        except:
            pass
        df = df.loc[index,:]
    return state.with_df(df)

@command
def In(state, command, column, *values):
    """In filter
    Accepts one column and zero or more values. Keep only rows where column equals one of the specified values.
    Example: In~column1~1~2~3
    """
    df = state.df()
    c = state.expand_columns([column])[0]

    index = None
    for v in values:
        state.log_info(f"Is in: {c} == {v}")
        if index is None:
            index = df[c] == v
        else:
            index = index | (df[c] == v)

        try:
            index = index | (df[c] == int(v))
        except:
            pass
        try:
            index = index | (df[c] == float(v))
        except:
            pass
    df = df.loc[index,:]
    return state.with_df(df)

@command
def Split(state, command, column):
    df = state.df()
    values = df[column].unique()
    state.data = ", ".join(values)
    return state

@command
def Set(state,command,name,value=True):
    """Set the state variable
    Sets a state variable with 'name' to 'value'. If 'value' is not specified, variable is set to True.
    """
    state.vars[name]=value
    return state

@command
def Link(state,command,linktype=None):
    """Return a link to the result.
    Linktype can be specified as parameter or as a state variable (e.g. Set~linktype~query)
    linktype can be
    - query : just a query from the state - (without the Link command)
    - dataurl : data URL (base64-encoded)
    - htmlimage :html image element
    """
    if linktype is None:
        linktype = state.vars.get("linktype","query")
    if linktype == "query":
        state.data = encode(parse(state.query)[:-1])
        state.extension = "txt"
    elif linktype == "dataurl":
        encoded = base64.b64encode(state.as_bytes()).decode('ascii')
        mime = state.mimetype()
        state.data = f'data:{mime};base64,{encoded}'
        state.extension = "txt"
    elif linktype == "htmlimage":
        encoded = base64.b64encode(state.as_bytes()).decode('ascii')
        mime = state.mimetype()
        state.data = f'<img src="data:{mime};base64,{encoded}"/>'
        state.extension = "html"

    return state
