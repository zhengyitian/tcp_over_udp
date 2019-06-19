from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from tornado.ioloop import PeriodicCallback
from TSreamClient2 import TStreamClient
import json,uuid
from helpFunc import *
from TOUManagerBase import TOUManagerBase

class TOUManagerClient(TCPServer,TOUManagerBase):
    def __init__(self):
        TCPServer.__init__(self)
        stream = TStreamClient('0.0.0.0',11223)    
        TOUManagerBase.__init__(self,stream)     
        self.listen(9999,'0.0.0.0')

    @gen.coroutine
    def handle_stream(self, stream, address):
        id = str(uuid.uuid1())
        pack = {'type':'conn','id':id}
        msg = TOUMsg(pack, '')
        print 'request',time.time()
        yield self.addTask(msg)
        msg = yield self.waitTaskBack(id)
        back = msg.m_json
        print 'request back',time.time(),back
        print 'connMap length:',len(self.connMap)

        if back['ret'] == 0:
            return
        conn_seq = back['conn_seq_back']
        self.addConnMap(conn_seq)   
        IOLoop.instance().add_callback(functools.partial(self.doRead,stream,conn_seq))
        IOLoop.instance().add_callback(functools.partial(self.doWrite,stream,conn_seq))
        

if __name__ == "__main__":
    t = TOUManagerClient()
    IOLoop.instance().add_callback(t.turnUp)    
    IOLoop.instance().start()
    
