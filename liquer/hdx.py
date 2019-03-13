from liquer import command
from liquer.state import State
import pandas as pd

class HdxState(State):
    def __init__(self):
        super().__init__()
        self.hxl_tags={}
    def hdx_resource(self, server, dataset_id, resource_id, filename=None):
        prefix = dict(prod="https://data.humdata.org/dataset/",
                      test="https://test-data.humdata.org/dataset/",
                      feature="https://feature-data.humdata.org/dataset/").get(server)

        url = f"{prefix}{dataset_id}/resource/{resource_id}/download"
        if filename is not None:
            url+="/"+filename
            self.with_filename(filename)
        self.add_source(url)
        return url


def load_hdx(state, server, dataset_id, resource_id, filename=None):
    url = state.hdx_resource(server,dataset_id,resource_id,filename)
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

@command
def hdx(state,dataset_id, resource_id, filename=None):
    """Load data from production HDX server.
    Resource must contain HXL tags in the first row.
    Arguments:
    dataset_id
    resource_id
    filename (optional) - resource file name; will become part of the request, but as well used as a default output file name
    """
    return load_hdx(state,"prod",dataset_id, resource_id, filename)

@command
def test_hdx(state,dataset_id, resource_id, filename=None):
    """Load data from test HDX server.
    Resource must contain HXL tags in the first row.
    Arguments:
    dataset_id
    resource_id
    filename (optional) - resource file name; will become part of the request, but as well used as a default output file name
    """
    return load_hdx(state,"test",dataset_id, resource_id, filename)

@command
def feature_hdx(state,dataset_id, resource_id, filename=None):
    """Load data from feature HDX server.
    Resource must contain HXL tags in the first row.
    Arguments:
    dataset_id
    resource_id
    filename (optional) - resource file name; will become part of the request, but as well used as a default output file name
    """
    return load_hdx(state,"feature",dataset_id, resource_id, filename)
