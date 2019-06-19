from tornado import gen
from pushClient import pushClient
from pullClient import pullClient
from statusClient import statusClient

#start by some policy(like start on the server first ,then on the client),
#process exits  auto when no heartbeats.

class UStreamClient():
    def __init__(self):
        pass
    
    def start(self):
        pass
    
    @gen.coroutine
    def read(self):
        pass
    
    @gen.coroutine
    def write(self,s):
        pass