from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from concurrent.futures import ThreadPoolExecutor
innerThreadPool = ThreadPoolExecutor(100)

coo = 0
class udpS():
    def __init__(self):
        self._read_buffer = ''
        pass
    
    @gen.coroutine        
    def connect(self):
        pass
    
    @gen.coroutine    
    def read_bytes(self,n):
        pass
    
    @gen.coroutine        
    def write(self,s):
        f = open('aa.txt','wb')
        f.write(s)
        f.close()
        print(s)
       
    
lastT = 0
@gen.coroutine
def cliEcho(streamS,streamC):
    while True:
        d1 = yield streamC.read_bytes(1)
        if len(d1)!=0:
            break
    
    data =  streamC._read_buffer
    l = len(data)
    d2 = yield streamC.read_bytes(l-1)
    t1 = time.time()
    global lastT
    print 'interval',t1-lastT
    lastT = t1
    yield streamS.write(d1+d2)
    tt = time.time()-t1
    global coo
    coo+=1
    print coo,'write1' ,l,tt
@gen.coroutine
def run_client(streamS,streamC):
    while True:
        try:
            yield cliEcho(streamS,streamC)
        except :
            print("server " + " left.")
            break
def sl(s):
    time.sleep(5)
    print s
    return 10
co = 0

@gen.coroutine
def tt():
    global co
    raise Exception
    if co != -1:
        return
    co += 1

    result = yield innerThreadPool.submit(sl, ('s',))
   
    yield gen.sleep(5)
    e = StopIteration()
    e.value = 11
    raise e        

class EchoServer(TCPServer): 
    clients = set()    
    @gen.coroutine
    def handle_stream(self, stream, address):
        ip, fileno = address
        print("Incoming connection from " + ip)
        EchoServer.clients.add(address)
        ip = '0.0.0.0'   
        try:
            x = yield tt()
        except:
            print 'exxcp'
        ip = '0.0.0.0'
        #ip = '154.92.15.210'
        streamCli = yield TCPClient().connect(ip, 4555)       
        #streamCli = udpS()
        print 'client ok'
        IOLoop.instance().add_callback(functools.partial(run_client,stream,streamCli))
         
        while True:
            try:
                yield self.echo(stream,streamCli)
            except :
                print("Client " + str(address) + " left.")
                EchoServer.clients.remove(address)
                break

    @gen.coroutine
    def echo(self, stream,streamCli):
        while True:
            d1 = yield stream.read_bytes(1)
            if len(d1)!=0:
                break
        data =  stream._read_buffer
        l = len(data)
        d2 = yield stream.read_bytes(l-1)
        t1 = time.time()
        yield streamCli.write(d1+d2)
        tt = time.time()-t1
        global coo
        coo+=1
        print coo,'write2' ,l,tt
        
def start_server():
    server = EchoServer()
    server.listen(9999,'0.0.0.0')

        

if __name__ == "__main__":
    start_server()
    IOLoop.instance().start()
