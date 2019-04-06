import os.path

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
    def store(self,state):
        self.storage[state.query]=state.clone()

class FileCache:
    def __init__(self,path):
        self.path=path
