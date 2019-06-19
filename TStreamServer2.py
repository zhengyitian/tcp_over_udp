from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.ioloop import PeriodicCallback
from tornado.tcpserver import TCPServer
from helpFunc import *
from collections import deque
import functools,time
ip = '0.0.0.0'
class TStreamServer(TCPServer):
    def __init__ (self,ip,port):
        super(TStreamServer,self).__init__()
        self.port = port
        self.ip = ip
        self.listen(self.port,self.ip)        
        self.isWrite = False
        self.isRead = False
        self.queue = deque()
        self.maxSize = 100*1024*1024
        self.stream = None
        #ti = PeriodicCallback(self.worker,10)
        #ti.start()
        self.isWorking = False
        self.thisSecond = 0
        self.hasWriteThisSecond = 0
        self.tripTime = 0
        self.maxWritePerSecond = 200*1024*1024
        self.currentCacheSize = 0    
        self.gotConn = False 
        self.lock = False
        
    @gen.coroutine
    def handle_stream(self, stream, address):
        if self.gotConn :
            return
        ip, fileno = address
        print("Incoming connection from " + ip)
        self.gotConn = True
        self.stream = stream


    @gen.coroutine
    def write(self,s):
        while not self.stream:
            yield gen.sleep(miniSleep)        
        yield self.stream.write(s)
     

    @gen.coroutine
    def read(self):
        while not self.stream:
            yield gen.sleep(miniSleep)        
        try:
            while True:
                d1 = yield self.stream.read_bytes(1)
                if len(d1)!=0:
                    break    
            data =  self.stream._read_buffer
            l = len(data)
            d2 = yield self.stream.read_bytes(l-1)
        except:
            print 'error'
            import sys
            sys.exit(0)
     
        torRet(d1+d2) 

co = 0
@gen.coroutine
def doRead(t):
    global co
    while True:
        yield gen.sleep(miniSleep)
        s = yield t.read()
        co += len(s)
        print time.time(),co,s[:20]     
        
@gen.coroutine
def doWrite(t):
    while True:
        yield gen.sleep(miniSleep)
        yield t.write( ('###%s###'%time.time())*100  )      

        
@gen.coroutine
def main(t):
    yield t.start()
    IOLoop.instance().add_callback(functools.partial(doRead,t))
    IOLoop.instance().add_callback(functools.partial(doWrite,t))

if __name__ == "__main__":
    t = TStreamServer('0.0.0.0',12345)
    IOLoop.instance().add_callback(functools.partial(main,t))
    IOLoop.instance().start()
