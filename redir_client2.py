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
        self.nextToSendPos = 0
        self.hasGot = {}
        self.hasSend = {}
        self.buff = {}
        self.maxPos = 0
        self.taskIds = {}
        self.isSending = {}
        self.readKeyBase = ''
        self.readStatusKey = ''
        self.statusKey = ''
        self.close = False
        self.writeTaskIds = {}
        self.peerPos = 0
        self.sendStatusKey = ''
        self.packSize = {}
        


    def sendStatus(self):
        pass
    @gen.coroutine    
    def read_bytes(self):
        self.work()
        while self.stall():
            self.work()
            yield gen.sleep(miniSleep)
        ret = self.clearBuffer()
        m = {'id':str(uuid.uuid1()),'nextToSendPos':self.nextToSendPos}
        task = ['set',self.sendStatusKey,json.dumps(m)]
        id = yield g_manager.add(task)     
        self.taskIds[id] = {'type':'setStatus'}
    def makeRange1(self,a,b):
        l = []
        if maxPack<self.nextToSendPos:
            l = range(self.nextToSendPos,self.maxPos+1)+ range(maxPack+1)
        if maxPack == self.nextToSendPos:
            l = [maxPack]
        if maxPack > self.nextToSendPos:
            l = range(self.nextToSendPos,maxPack+1)        
        pass
    def makeRange2(self,a,l):
        pass    
    def circleBig(self,a,b):
        pass
    
    def circleMax(self,l):
        pass
    def circleAdd(self,a,b):
        pass
    def clearBuffer(self):
        ss = ''
        while True:
            if self.nextToSendPos not in self.hasGot:
                break
            ss += self.buff[self.nextToSendPos]
            del self.buff[self.nextToSendPos]
            del self.hasGot[self.nextToSendPos]
            del self.hasSend[self.nextToSendPos]
            if self.nextToSendPos == self.maxPos:
                self.nextToSendPos = 0
            else:
                self.nextToSendPos += 1
        return ss

    def stall(self):
        if self.close:
            return
        if self.nextToSendPos in self.hasGot:
            return True

    def work(self):
        pack_to_send = []
        keys = self.taskIds.keys()
        for k in keys:
            if k in g_manager.outputMap:
                r = g_manager.get(k)
                con = self.taskIds[k]
                if con['type']=='readPack':
                    pack = con['pack']
                    del self.isSending[pack]
                    if r:
                        self.hasGot[pack] = 1
                        self.buff[pack] = r         
        keys = self.taskIds.keys()
        for k in keys:
            if k in g_manager.outputMap:
                r = g_manager.get(k)
                con = self.taskIds[k]                        
                if con['type']=='readStatus':
                    if con['close'] == 1:
                        self.deal_close()
                        return
                    maxPack = con['maxPack']
                    yourPos = con['yourPos']
                    size = con['size']
                    l = self.makeRange1(yourPos,maxPack)
                    for i in l:
                        self.packSize[i] = size[i]
                    if self.circleBig(maxPack,self.peerPos):
                        self.peerPos = maxPack
        b = self.circleMax(self.hasGot)
        ma = b
        if self.circleBig(self.peerPos,b):
            ma = self.peerPos
        l = self.makeRange1(self.nextToSendPos,ma)
        
        for one in l:
            if one not in self.isSending and one not in pack_to_send:
                pack_to_send.append(one)
        l = self.makeRange2(ma,self.readAhead)
        for one in l:
            if one not in self.hasSend and one not in pack_to_send:
                pack_to_send.append(one)
            
        for one in pack_to_send:
            self.sendAll(one)
          
            
        if not pack_to_send and not self.isSending:
            st = self.nextToSendPos
            co = 0
            ll = []
            while co<self.readAhead:
                if st not in self.hasGot:
                    co +=1
                    self.sendAll(st)
                    st = self.circleAdd(st,1)
                
                 
    def sendAll(self,pack):
        task = ['get',self.readKeyBase+str(one),self.packSize[one]]
        id = yield g_manager.add(task)
        self.taskIds[id] = {'type':'readPack','pack':one}
        self.isSending[one] = 1
        self.hasSend[one] = 1
        task = ['get',self.readStatusKey,2]
        id = yield g_manager.add(task)
        self.taskIds[id] = {'type':'readStatus'}
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
