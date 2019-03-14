from liquer import *
import pandas as pd
import io
from urllib.request import urlopen

@command
def FROM(state, command, *url):
    """Load data from URL"""
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
def append(state, command, *url):
    """Append data from URL"""
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

@command
def echo(state, command, *txt):
    state.data=" ".join(txt)
    state.extension = "txt"
    return state

@command
def keep_cols(state, command, *columns):
    c = state.expand_columns(columns)
    state.log_info("Keep columns: "+(", ".join(c)))
    df = state.df()
    return state.with_df(df.loc[:,c])

@command
def rm_cols(state, command, *columns):
    remove = state.expand_columns(columns)
    state.log_info("Remove columns: "+(", ".join(remove)))
    keep = [c for c in state.columns if c not in remove]

    df = state.df()
    return state.with_df(df.loc[:,keep])

@command
def eq(state, command, *column_values):
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
def is_in(state, command, column, *values):
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
