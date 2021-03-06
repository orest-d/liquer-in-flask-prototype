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
            state.log_info(f"Chart XY ({xcol} {ycol})")
            axis.plot(df[xcol],df[ycol],label=state.column_label(ycol))
            continue
        elif t == "xye":
            xcol = state.expand_column(series.pop())
            ycol = state.expand_column(series.pop())
            ecol = state.expand_column(series.pop())
            state.log_info(f"Chart XY ({xcol} {ycol}) Error:{ecol}")
            axis.errorbar(df[xcol],df[ycol],yerr=df[ecol],label=state.column_label(ycol))
            continue
        elif t == "xyee":
            xcol = state.expand_column(series.pop())
            ycol = state.expand_column(series.pop())
            e1col = state.expand_column(series.pop())
            e2col = state.expand_column(series.pop())
            state.log_info(f"Chart XY ({xcol} {ycol}) Error:({e1col},{e2col})")
            axis.errorbar(df[xcol],df[ycol],yerr=[df[e1col],df[e2col]],label=state.column_label(ycol))
            continue
        elif t == "cxy":
            c = series.pop()
            xcol = state.expand_column(series.pop())
            ycol = state.expand_column(series.pop())
            axis.plot(df[xcol],df[ycol],c,label=state.column_label(ycol))
            continue
        else:
            state.log_warning(f"Unrecognized MPL parameter {t}")
    fig.legend()
    output = io.BytesIO()
    fig.savefig(output, dpi=300, format=extension)
    state.data = output.getvalue()
    state.extension=extension
    return state

@df_source
def TestData():
    """Test data
    """
    x=np.linspace(-5,5,100)
    return pd.DataFrame(dict(x=x,y=np.sin(x),y1=0.1*np.sin(x),y2=0.2*np.sin(x+0.1)))
