from liquer import *
import pandas as pd
import io
from urllib.request import urlopen

@command
def FROM(state, *url):
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
    state.data = df
    state.with_columns(df.columns)
    return state

