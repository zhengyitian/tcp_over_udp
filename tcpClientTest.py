from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time


@gen.coroutine
def run_client(stream):
    while True:
        s = 'hehe'*1024*1024
        yield stream.write(s)
        print time.time(),'write ok'

        
@gen.coroutine
def main():
    ip = '0.0.0.0'      
    stream = yield TCPClient().connect(ip, 9999)       
    IOLoop.instance().add_callback(functools.partial(run_client,stream))
     


if __name__ == "__main__":
    IOLoop.instance().add_callback(main)
    IOLoop.instance().start()
