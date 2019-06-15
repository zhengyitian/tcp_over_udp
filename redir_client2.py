from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from concurrent.futures import ThreadPoolExecutor
innerThreadPool = ThreadPoolExecutor(100)
from tornado.ioloop import PeriodicCallback
import threading,uuid,copy,json,random
miniSleep = 0.01

firstKey = 'firstKey'
connReplyKey = 'connReplyKey'
isStart = True
conn_seq = -1

def torRet(r):
    e = StopIteration()
    e.value = r
    raise e  

import redis
conn = redis.Redis(host='localhost', port=6379, db=0)
def re_get(k):
    global conn
    try:
        return conn.get(k)
    except:
        conn = redis.Redis(host='localhost', port=6379, db=0)
        return conn.get(k)        

def re_set(k,s):
    global conn
    try:
        conn.set(k,s)
    except:
        conn = redis.Redis(host='localhost', port=6379, db=0)
        conn.set(k,s)
def re_del(k):
    global conn
    try:
        conn.delete(k)
    except:
        conn = redis.Redis(host='localhost', port=6379, db=0)
        conn.delete(k)

class redisManager():
    def __init__(self):
        self.inputMap = {}
        self.outputMap = {}
        self.maxSize = 100
        self.delMap = {}
        
    def addDel(self,id):
        self.delMap[id] = 0
        
    @gen.coroutine
    def add(self,task):
        while len(self.inputMap)>self.maxSize:
            yield gen.sleep(miniSleep)
        id = str(uuid.uuid1())
        self.inputMap[id] = {'task':task,'createTime':time.time()}
        torRet(id)
    def get(self,id):
        if id not in self.outputMap:
            return 
        r = self.outputMap[id]
        del self.outputMap[id]
        return r
    
    @gen.coroutine
    def synFunc(self,task):
        task = ['get',firstKey,1]
        id = yield self.add(task)
        while id not in self.outputMap:
            yield gen.sleep(miniSleep)
        r = self.get(id)
        torRet(r)        
        
    def doWork(self):
        for k,v in self.inputMap.items():
            if k in self.delMap:
                del self.delMap[k]
                del self.inputMap[k]
                continue
            if v['createTime'] +random.randint(100,200)*0.2/100.0<time.time():
                opt,k2,v2 = v['task']
                ret = ''
                if opt == 'get':
                    ret = re_get(k2)
                if opt == 'set':
                    ret = re_set(k2, v2)
                    ret = 'ok'
            self.outputMap[k] = ret
            del self.inputMap[k]

g_manager = redisManager()

class udpSW():
    def __init__(self,parent):
        self.parent = parent
        self.conn_uuid = ''
        self.wirteUuid = ''
        self.wirteKeyBase = ''
        self.writeCircle = 0  # 0 ir 1
        self.writeCircle_peer = 0
        self.writeCircleLen = 0
        self.writePos = 0
        self.writePos_peer = 0
        self.statusKey = ''
        self.close = False
        self.writeTaskIds = {}
  

    def stall(self):
        if self.close:
            return
        if self.writePos == self.writeCircleLen-1 and self.writeCircle!=self.writeCircle_peer:
            return True
    def work(self):
        keys = self.writeTaskIds.keys()
        for k in keys:
            if k in g_manager.outputMap:
                r = g_manager.get(k)
                del self.writeTaskIds[k]
                if r == None:
                    return
                m = json.loads(r)
                if m['close'] == 1:
                    self.close = True
                    return
                self.writeCircle_peer = m['writeCircle_peer']
                self.writePos_peer = m['writePos_peer']
    def deal_close(self):
        pass
    @gen.coroutine        
    def write(self,s):
        self.work()  
        while self.stall():
            self.work()  
            yield gen.sleep(miniSleep)
        if self.close:
            self.deal_close()
            raise Exception
        key = self.wirteKeyBase+str(self.writeCircle*self.writeCircleLen+self.writePos)
        val = self.wirteUuid+s
        id = yield g_manager.add(['set',key,val])
        self.writeTaskIds[id]=0
        id = yield g_manager.add(['get',self.statusKey,1])
        self.writeTaskIds[id]=0
        if self.writePos == self.writeCircleLen-1:
            self.writePos = 0
            if self.writeCircle == 0:
                self.writeCircle = 1
            else:
                self.writeCircle = 0            

class udpSR():
    def __init__(self,parent):
        self.parent = parent
        self.readAhead = 10
        self.readCircleLen = 0 #like 200
        self.maxRec = 0
        self.clearPos = 0
        self.hasRecPos = 0
        
        self.conn_uuid = ''
        self.wirteUuid = ''
        self.wirteKeyBase = ''
        self.writeCircle = 0  # 0 ir 1
        self.writeCircle_peer = 0
        self.writeCircleLen = 0
        self.writePos = 0
        self.writePos_peer = 0
        self.statusKey = ''
        self.close = False
        self.writeTaskIds = {}
        self.peerPos = 0
        
    def isSending(self):
        pass
    def hasGot(self):
        pass
    def clearBuffer(self):
        pass
    def getPeerStatus(self):
        pass
    def sendStatus(self):
        pass
    @gen.coroutine    
    def read_bytes(self):
        self.work()
        while self.stall():
            self.work()
            yield gen.sleep(miniSleep)
        
        ret = self.clearBuffer()
        for i in range(self.clearPos,self.clearPos+self.readAhead):
            if not self.isSending(i) and not self.hasGot(i):
                g_manager.add(['get','xxoo',1])
            self.getPeerStatus()
            self.sendStatus()
        
    def stall(self):
        if self.close:
            return
        if self.writePos == self.writeCircleLen-1 and self.writeCircle!=self.writeCircle_peer:
            return True
    def work(self):
        if get_stauts:
            for i in range(self.hasRecPos,self.peerPos+10):
                g_manager.add(['get','xxoo',1])
            self.getPeerStatus()
            self.sendStatus()
        if rec_get:
            if aheadPac_just_send:
                pass
            pass
        keys = self.writeTaskIds.keys()
        for k in keys:
            if k in g_manager.outputMap:
                r = g_manager.get(k)
                del self.writeTaskIds[k]
                if r == None:
                    return
                m = json.loads(r)
                if m['close'] == 1:
                    self.close = True
                    return
                self.writeCircle_peer = m['writeCircle_peer']
                self.writePos_peer = m['writePos_peer']
    def deal_close(self):
        pass

class udpS():
    def __init__(self):
        self.readS = udpSR(self)
        self.writeS = udpSW(self)
        
    @gen.coroutine        
    def connect(self):
        global conn_seq
        conn_seq += 1
        yield g_manager.synFunc(['set',firstKey,str(conn_seq)])
        while True:
            r = yield  g_manager.synFunc(['get',connReplyKey])
            m = json.loads(r)
            if m['seq'] == conn_seq:
                break
        if m['status'] != 1:
            torRet(False)
        self.writeS.conn_uuid = m['conn_uuid']
        self.writeS.wirteUuid = m['wirteUuid']
        self.writeS.wirteKeyBase = m['wirteKeyBase']
        self.writeS.writeCircleLen = m['writeCircleLen']
        self.writeS.currentCircle = self.writeS.writeCircle = 0
        torRet(True)        
        

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
    def __init__(self):
        super(EchoServer, self).__init__()
        self.connSeq = -1
  
    @gen.coroutine
    def handle_stream(self, stream, address):
        global isStart,conn_seq 
        if isStart:
            isStart = False
            conn_seq = yield g_manager.synFunc(['get',firstKey,1])    
        us = udpS()
        re = yield us.connect()
        if not re:
            stream.close()
            return
        print 'client ok'
        readS = us.readS
        writeS = us.writeS
        IOLoop.instance().add_callback(functools.partial(run_client,stream,readS))
        while True:
            try:
                yield self.echo(stream,writeS)
            except StreamClosedError:
                print("Client  left.")
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
    managerTimer  = PeriodicCallback(g_manager.doWork,10)
    managerTimer.start()    
    IOLoop.instance().start()
