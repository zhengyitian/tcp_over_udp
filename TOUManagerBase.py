from tornado.ioloop import IOLoop
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from tornado.ioloop import PeriodicCallback
import json,uuid
from helpFunc import *

class TOUManagerBase():
    def __init__(self,stream):     
        self.stream = stream
        work1  = PeriodicCallback(self.stream_to_map,10)
        work1.start()
        IOLoop.instance().add_callback(self.toStream)
        self.outputMap_byConn = {}
        self.outputMap_byId = {}
        self.maxCacheSize = 100*1024*1024
        self.outputSize = 0
        self.connMap = {}
        self.eachConnWriteLimit = 10*1024*1000
        self.streamReadLeft = ''
        self.inputCache = ''
        self.inputCacheSizeLimit = 10*1024*1024
        self.lock1 = False
        
    def addConnMap(self,conn_seq):
        m={}
        m['readError'] = False
        m['writeError'] = False
        m['writeNotBack'] = 0
        m['readBuffer'] = ''
        m['write_seq_addTask'] = 0
        m['write_seq_doWrite'] = 0
        self.connMap[conn_seq] = m          
        
    @gen.coroutine
    def toStream(self):
        while True:
            s = self.inputCache
            if not s:
                yield gen.sleep(miniSleep)
                continue
            self.inputCache = ''
            yield self.stream.write(s)
            
    @gen.coroutine
    def waitTaskBack(self,id):
        while id not in self.outputMap_byId.keys():
            yield gen.sleep(miniSleep)
        msg = self.outputMap_byId[id]
        del self.outputMap_byId[id]
        self.outputSize -= msg.length
        torRet(msg)       
        
    @gen.coroutine
    def turnUp(self):
        yield self.stream.start()
        
    @gen.coroutine
    def addTask(self,msg):
        while len(self.inputCache)>self.inputCacheSizeLimit:
            yield gen.sleep(miniSleep)
        j = msg.m_json
        ty = j.get('type','')
        if ty=='write':
            conn_seq = j['conn_seq']
            write_seq = j['write_seq']
            while True:
                if conn_seq not in self.connMap:
                    return
                if write_seq != self.connMap[conn_seq]['write_seq_addTask']:
                    yield gen.sleep(miniSleep)
                    continue
                break
            self.connMap[conn_seq]['write_seq_addTask'] = circleAddOne(self.connMap[conn_seq]['write_seq_addTask'])
        self.inputCache += msg.pack()


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

            con = {'type':'write','conn_seq':conn_seq,'write_seq':self.connMap[conn_seq]['write_seq_doWrite']}
            self.connMap[conn_seq]['write_seq_doWrite'] = circleAddOne(self.connMap[conn_seq]['write_seq_doWrite'])
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
                continue
            if conn_seq not in self.connMap :
                return             
            s = self.connMap[conn_seq]['readBuffer']
            self.connMap[conn_seq]['readBuffer'] = ''
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
        if self.lock1:
            return
        self.lock1 = True
        s = yield self.stream.read()
        #print 'got from stream',time.time(),len(s)
        self.streamReadLeft += s
        while True:       
            if self.outputSize > self.maxCacheSize:
                self.lock1 = False
                return
            msg = TOUMsg()
            r,self.streamReadLeft = msg.unpack(self.streamReadLeft)
            if not r:
                self.lock1 = False
                return
            
            #print 'got msg',time.time(),msg.m_json
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
            
          

