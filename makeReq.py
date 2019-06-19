from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time



@gen.coroutine
def a():
    stream = yield TCPClient().connect('0.0.0.0', 7777)       
    print 'client ok'
    f = open('a.txt','rb')
    s = f.read()
    f.close()
    print len(s)
    yield stream.write(s)
    
    while True:
        d1 = yield stream.read_bytes(1)
        if len(d1)!=0:
            break
    data =  stream._read_buffer
    l = len(data)
    d2 = yield stream.read_bytes(l-1)
    s = d1+d2
    print s
  

if __name__ == "__main__":
    IOLoop.instance().add_callback(a)
    IOLoop.instance().start()
