from tornado import gen
from pushServer import pushServer
from pullServer import pullServer
from statusServer import statusServer

#start by some policy(like start on the server first ,then on the client),
#process exits  auto when no heartbeats.

class UStreamServer():
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