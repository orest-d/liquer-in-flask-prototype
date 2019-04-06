from liquer import *
import pandas as pd
import io
from urllib.request import urlopen
from matplotlib.figure import Figure
from matplotlib import pyplot as plt
import numpy as np


@command
def MPL(state, command, *series):
    """Matplotlib chart
    """
    fig = plt.figure(figsize=(8, 6), dpi=300)
    axis = fig.add_subplot(1, 1, 1)
    series = list(reversed(list(series)))
    extension="png"
    df = state.df()
    while len(series):
        t = series.pop()
        if t in ["jpg","png","svg"]:
            extension = t
            continue
        elif t == "xy":
            xcol = state.expand_column(series.pop())
            ycol = state.expand_column(series.pop())
            axis.plot(df[xcol],df[ycol],label=state.column_label(ycol))
            continue
        elif t == "cxy":
            c = series.pop()
            xcol = state.expand_column(series.pop())
            ycol = state.expand_column(series.pop())
            axis.plot(df[xcol],df[ycol],c,label=state.column_label(ycol))
            continue
        else:
            state.log_warning(f"Unrecognized MPL parameter {t}")
     
    output = io.BytesIO()
    fig.savefig(output, dpi=300, format=extension)
    state.data = output.getvalue()
    state.extension=extension
    return state

