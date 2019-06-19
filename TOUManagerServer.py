#from UStreamClient import UStreamClient
#from UStreamServer import UStreamServer
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from tornado.ioloop import PeriodicCallback
from TStreamServer2 import TStreamServer
import json,uuid
from helpFunc import *
from TOUManagerBase import TOUManagerBase

class TOUManagerServer(TOUManagerBase):
    def __init__(self):
        stream = TStreamServer('0.0.0.0',11223)    
        TOUManagerBase.__init__(self,stream)
        work2  = PeriodicCallback(self.acceptConn,10)
        work2.start()              
        self.isDoingAccept = False
        
    @gen.coroutine
    def acceptConn(self):
        if self.isDoingAccept :
            return
        self.isDoingAccept = True
        for k,msg in self.outputMap_byId.items():
            v = msg.m_json
            if v['type'] == 'conn':
                try:
                    stream = yield TCPClient().connect('0.0.0.0', 8080)    
                except:
                    m = {'ret':0,'id':k}
                    msg2 = TOUMsg(m)
                    yield self.addTask(msg2)
                    self.outputSize -= msg.length
                    del self.outputMap_byId[k]
                    continue
                
                print 'accept,connMap length:',len(self.connMap)                
                conn_seq_back = str(uuid.uuid1())
                m = {'ret':1,'id':k,'conn_seq_back':conn_seq_back}
                msg2 = TOUMsg(m)
                yield self.addTask(msg2)
                self.outputSize -= msg.length
                del self.outputMap_byId[k]
                self.addConnMap(conn_seq_back)

                IOLoop.instance().add_callback(functools.partial(self.doRead,stream,conn_seq_back))
                IOLoop.instance().add_callback(functools.partial(self.doWrite,stream,conn_seq_back))                
                        
        self.isDoingAccept = False
                
if __name__ == "__main__":
    t = TOUManagerServer()
    IOLoop.instance().add_callback(t.turnUp)    
    IOLoop.instance().start()
    
