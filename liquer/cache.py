import json
import os.path
from os import makedirs
import hashlib
from liquer.state import State 

class NoCache:
    def get(self,key):
        return None
    def store(self,state, final_state=True):
        return None

class MemoryCache:
    def __init__(self):
        self.storage={}
    def get(self,key):
        return self.storage.get(key).clone()
    def store(self,state, final_state=True):
        self.storage[state.query]=state.clone()

class FileCache:
    def __init__(self,path):
        self.path=path
        try:
            makedirs(path)
        except FileExistsError:
            pass

    def to_path(self,key):
        m = hashlib.md5()
        m.update(key.encode('utf-8'))
        digest = m.hexdigest()
        return os.path.join(self.path,digest)

    def get(self,key):
        state_path = self.to_path(key)+".json"
        if os.path.exists(state_path):
            state = State()
            state = state.from_state(json.loads(state_path))
        else:
            return None
        csv_path = self.to_path(key)+".csv"
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            return state.with_df(df)

    def store(self, state, final_state=True):
        try:
            df = state.df()
        except:
            return
        with open(self.to_path(state.query)+".json","w") as f:
            f.write(json.dumps(state.state()))
        df.to_csv(self.to_path(state.query)+".csv",index=False)
    