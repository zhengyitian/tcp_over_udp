from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools

@gen.coroutine
def cliEcho(streamS,streamC):
    while True:
        d1 = yield streamC.read_bytes(1)
        if len(d1)!=0:
            break
    
    data =  streamC._read_buffer
    l = len(data)
    d2 = yield streamC.read_bytes(l-1)
    yield streamS.write(d1+d2)
    
@gen.coroutine
def run_client(streamS,streamC):
    while True:
        try:
            yield cliEcho(streamS,streamC)
        except StreamClosedError:
            print("server " + " left.")
            break
        
class EchoServer(TCPServer): 
    clients = set()    
    @gen.coroutine
    def handle_stream(self, stream, address):
        ip, fileno = address
        print("Incoming connection from " + ip)
        EchoServer.clients.add(address)
      
        ip = '154.92.15.210'      
        ip = '0.0.0.0'      
        streamCli = yield TCPClient().connect(ip, 8080)          
        print 'client ok'
        IOLoop.instance().add_callback(functools.partial(run_client,stream,streamCli))
         
        while True:
            try:
                yield self.echo(stream,streamCli)
            except StreamClosedError:
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
        yield streamCli.write(d1+d2)

def start_server():
    server = EchoServer()
    server.listen(9999,'0.0.0.0')

        

if __name__ == "__main__":
    start_server()
    IOLoop.instance().start()
