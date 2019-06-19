from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
ip = '0.0.0.0'

@gen.coroutine
def run_client(stream):
    while True:
        try:
            while True:
                d1 = yield stream.read_bytes(1)
                if len(d1)!=0:
                    break    
            data =  stream._read_buffer
            l = len(data)
            d2 = yield stream.read_bytes(l-1)
            print time.time(), len(d1+d2) 
            yield stream.write(d1+d2)
        except:
            break
        
class EchoServer(TCPServer): 
    @gen.coroutine
    def handle_stream(self, stream, address):
        ip, fileno = address
        print("Incoming connection from " + ip)
        IOLoop.instance().add_callback(functools.partial(run_client,stream))
        
        print 'client ok'

def start_server():
    server = EchoServer()
    server.listen(9999,'0.0.0.0')

        

if __name__ == "__main__":
    start_server()
    IOLoop.instance().start()
