from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.ioloop import PeriodicCallback
from tornado.tcpserver import TCPServer
import random
import functools,time
from helpFunc import *
co = 0

@gen.coroutine
def a():
    global co
    co += 1
    temp = co
    print 'call in :',co
    yield gen.sleep(random.randint(1,1000)/1000.0)
    print 'call out:',temp
    
work1  = PeriodicCallback(a,1)
work1.start()
IOLoop.instance().start()