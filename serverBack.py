#from UStreamClient import UStreamClient
#from UStreamServer import UStreamServer
from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from tornado.ioloop import PeriodicCallback
from TSreamClient import TStreamClient
from TStreamServer import TStreamServer
import json,uuid
from helpFunc import *

class TOUManagerServer():
    def __init__(self):
        #self.ustream = UStreamClient()
        self.stream = TStreamServer('0.0.0.0',11223)        
        work1  = PeriodicCallback(self.stream_to_map,10)
        work1.start()
        work2  = PeriodicCallback(self.acceptConn,10)
        work2.start()        
        self.outputMap_byConn = {}
        self.outputMap_byId = {}
        self.maxCacheSize = 10*1024*1024
        self.outputSize = 0
        self.connMap = {}
        self.eachConnWriteLimit = 1024*1024
        self.streamReadLeft = ''
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
                conn_seq_back = str(uuid.uuid1())
                m = {'ret':1,'id':k,'conn_seq_back':conn_seq_back}
                msg2 = TOUMsg(m)
                yield self.addTask(msg2)
                self.outputSize -= msg.length
                del self.outputMap_byId[k]
                m={}
                m['readError'] = False
                m['writeError'] = False
                m['writeNotBack'] = 0
                m['readBuffer'] = ''
                self.connMap[conn_seq_back] = m        
                IOLoop.instance().add_callback(functools.partial(self.doRead,stream,conn_seq_back))
                IOLoop.instance().add_callback(functools.partial(self.doWrite,stream,conn_seq_back))                
                        
        self.isDoingAccept = False
                
    @gen.coroutine
    def start(self):
        yield self.stream.start()
        
    @gen.coroutine
    def addTask(self,msg):
        yield self.stream.write(msg.pack())
        
    @gen.coroutine
    def waitTaskBack(self,id):
        while id not in self.outputMap_byId.keys():
            yield gen.sleep(miniSleep)
        msg = self.outputMap_byId[id]
        del self.outputMap_byId[id]
        self.outputSize -= msg.length
        torRet(msg)
        
    @gen.coroutine
    def handle_stream(self, stream, address):
        id = str(uuid.uuid1())
        pack = {'type':'conn','id':id}
        msg = TOUMsg(pack, '')
        yield self.addTask(msg)
        msg = yield self.waitTaskBack(id)
        back = msg.m_json
        if back['ret'] == 0:
            return
        conn_seq = back['conn_seq_back']
        m['readError'] = False
        m['writeError'] = False
        m['writeNotBack'] = 0
        m['readBuffer'] = ''
        self.connMap[conn_seq] = m        
        IOLoop.instance().add_callback(functools.partial(self.doRead,stream,conn_seq))
        IOLoop.instance().add_callback(functools.partial(self.doWrite,stream,conn_seq))
        
    def checkDelConn(self,conn_seq):
        if self.connMap[conn_seq]['writeError'] and self.connMap[conn_seq]['readError']:
            self.outputSize -= len(self.connMap[conn_seq]['readBuffer'])
            del self.connMap[conn_seq]        
            
    @gen.coroutine
    def doWrite(self,stream,conn_seq):
        while True:
            if conn_seq not in self.connMap:
                return
            try:
                while True:
                    d1 = yield stream.read_bytes(1)
                    if len(d1)!=0:
                        break
                data =  stream._read_buffer
                l = len(data)
                d2 = yield stream.read_bytes(l-1)
            except StreamClosedError:                
                pack = {'type':'readError','conn_seq':conn_seq}
                msg = TOUMsg(pack,'')
                yield self.addTask(msg)
                if conn_seq not in self.connMap:
                    return                
                self.connMap[conn_seq]['writeError'] = True
                self.checkDelConn(conn_seq)              
                return
            except :
                raise Exception
            
            if conn_seq not in self.connMap :
                return 
            while True:
                if conn_seq not in self.connMap :
                    return  
                if self.connMap[conn_seq]['writeError']:
                    return
                if self.connMap[conn_seq]['writeNotBack']<self.eachConnWriteLimit:
                    break
                yield gen.sleep(miniSleep)
                
            con = {'type':'write','conn_seq':conn_seq}
            msg = TOUMsg(con,d1+d2)
            self.connMap[conn_seq]['writeNotBack'] = self.connMap[conn_seq]['writeNotBack']+len(d1+d2)
            yield self.addTask(msg)

    @gen.coroutine
    def doRead(self,stream,conn_seq):    
        while True:
            if conn_seq not in self.connMap :
                return 
            if self.connMap[conn_seq]['readBuffer'] =='' and self.connMap[conn_seq]['readError'] :
                return
            if self.connMap[conn_seq]['readBuffer'] =='' :
                yield gen.sleep(miniSleep)
            s = self.connMap[conn_seq]['readBuffer']
            self.outputSize -= len(s)
            
            try:
                yield stream.write(s)
                con = {'type':'writeBack','conn_seq':conn_seq,'length':len(s)}
                msg = TOUMsg(con,'')
                yield self.addTask(msg)                
                
            except StreamClosedError:
                pack = {'type':'writeError','conn_seq':conn_seq}
                msg = TOUMsg(pack,'')
                yield self.addTask(msg)
                if conn_seq not in self.connMap:
                    return                
                self.connMap[conn_seq]['readError'] = True
                self.checkDelConn(conn_seq)
                return                
            except :
                raise Exception
        
    @gen.coroutine
    def stream_to_map(self):
        s = yield self.stream.read()
        self.streamReadLeft += s
        while True:       
            if self.outputSize > self.maxCacheSize:
                return
            msg = TOUMsg()
            r,self.streamReadLeft = msg.unpack(self.streamReadLeft)
            if not r:
                break

            json = msg.m_json
            if 'conn_seq' not in json:
                self.outputMap_byId[json['id']] = msg
                self.outputSize += msg.length
                continue

            ty = json['type']
            conn_seq = json['conn_seq']
            if conn_seq not in self.connMap:
                continue
            if ty == 'write':
                self.connMap[conn_seq]['readBuffer'] = self.connMap[conn_seq]['readBuffer']+msg.strContetn
                self.outputSize += len(msg.strContetn)
            elif ty == 'readError':
                self.connMap[conn_seq]['readError'] = True
            elif ty == 'writeBack':
                self.connMap[conn_seq]['writeNotBack'] = self.connMap[conn_seq]['writeNotBack'] -json['length']
            elif ty == 'writeError':
                self.connMap[conn_seq]['writeError'] = True 
            self.checkDelConn(conn_seq)
          

if __name__ == "__main__":
    t = TOUManagerServer()
    IOLoop.instance().add_callback(t.start)    
    IOLoop.instance().start()
    
