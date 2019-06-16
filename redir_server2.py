from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from concurrent.futures import ThreadPoolExecutor
import json
import base64
import re 
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


packLimit = 700
sleepTime = 0.01
recLen = 10240
connRecord = {}
availPort = {}
import random,uuid
firstKey = 'firstKey'
connReplyKey = 'connReplyKey'
conn_seq = 0
re_set(firstKey,str(conn_seq))
connKyeBase = 'connKyeBase'
miniSleep = 0.01
def torRet(r):
    e = StopIteration()
    e.value = r
    raise e  


class udpSR():
    def __init__(self,parent):
        self.parent = parent
        self.readCircleLen = 300
        self.readKeyBase = str(uuid.uuid1())
        self.clientReadStatusKey = str(uuid.uuid1())
        self.close = False
        self.clientPos = 0
        self.clientSendStatusKey = str(uuid.uuid1())
        self.writeAhead = 50
        self.serverPos = 0
        self.buff = {}
    def doClose(self):
        pass
    def ini(self,m):
        m['readCircleLen']=self.readCircleLen
        m['readKeyBase']=self.readKeyBase
        m['clientReadStatusKey']=self.clientReadStatusKey
        m['clientSendStatusKey']=self.clientSendStatusKey
        return m
    
    def circleRange(self,a,b):  # return [,)  same as range
        temp = a
        ret = []
        while True:
            if not self.circleBig(b,temp):
                break
            ret.append(temp)
            temp = self.circleAdd(temp,1)
        return ret

    def circleBig(self,a,b):
        if a==b:
            return False
        if a>b and (a-b)<self.readCircleLen/2:
            return True
        if a<b and (b-a)>self.readCircleLen/2:
            return True
        return False
    
    def circleAddOne(self,a):
        if a == self.readCircleLen-1:
            return 0
        return a+1
        
    def circleAdd(self,a,b):
        ret = a
        for i in range(b):
            ret = self.circleAddOne(ret)
        return ret
    def refreshPeerStatus(self):
        r = re_get(self.clientSendStatusKey)
        if not r:
            return
        m = json.loads(r)
        newPos = m['clientPos']
        if not self.circleBig(newPos,self.clientPos):
            return
        l = self.circleRange(self.clientPos,newPos)
        for i in l:
            del self.buff[i]
        self.clientPos = newPos

    def doWrite(self,s):
        self.buff[self.serverPos] = {'con':s,'size':int(len(s)/packLimit)+1}
        key = self.readKeyBase+str(self.serverPos)
        re_set(key,s)
        self.serverPos = self.circleAdd(self.serverPos,1)
        
    def refreshSelfStatus(self):
        m = {}
        m ['close'] = 0
        m['clientPos'] = self.clientPos
        m['serverPos'] = self.serverPos
        l = self.circleRange(self.clientPos,self.serverPos)
        packSize = {}
        for one in l:
            packSize[one] = self.buff[one]['size']
        m['packSize'] = packSize
        j = json.dumps(m)
        re_set(self.clientReadStatusKey,j)
    
    def work(self):
        self.refreshPeerStatus()
    
    def stall(self):
        limitPos = self.circleAdd(self.clientPos,self.writeAhead)
        if self.circleBig(self.serverPos,limitPos):
            return True
        return False
    
    @gen.coroutine        
    def write(self,s):
        self.work()
        while True:
            if not self.stall():
                break
            yield gen.sleep(miniSleep)
            self.work()
        self.doWrite(s)
        self.refreshSelfStatus() 
        
class udpSW():
    def __init__(self,parent):
        self.parent = parent
        self.wirteKeyBase = ''
        self.writeCircle_peer = 0
        self.writeCircleLen = 50
        self.writePos_peer = 0
        self.statusKey = str(uuid.uuid1())
        self.seq = 0
        self.seqLimit = 10000
    def ini(self,m):
        self.wirteKeyBase = str(uuid.uuid1())
        self.writeCircleLen = 100
        self.statusKey = str(uuid.uuid1())
        m['wirteKeyBase'] = self.wirteKeyBase
        m['writeCircleLen'] = self.writeCircleLen
        m['statusKey'] = self.statusKey    
        return m
    def refreshStatus(self):
        m = {}
        m['writeCircle_peer'] = self.writeCircle_peer
        m['writePos_peer'] = self.writePos_peer
        m['seq'] = self.seq
        m['close'] = 0
        j = json.dumps(m)
        re_set(self.statusKey,j)
        
    def work(self):
        ss = ''
        while True:
            key = self.wirteKeyBase+str(self.writeCircle_peer*self.writeCircleLen+self.writePos_peer)
            r = re_get(key)
            if not r:
                break
            ss += r
            re_set(key,'')
            if self.writePos_peer == self.writeCircleLen-1:
                self.writePos_peer = 0
                self.seq += 1
                if self.seq == self.seqLimit:
                    self.seq = 0
                if self.writeCircle_peer == 0:
                    self.writeCircle_peer = 1
                else:
                    self.writeCircle_peer = 0  
            else:
                self.writePos_peer += 1
            return ss
    
    def stall(self):
        key = self.wirteKeyBase+str(self.writeCircle_peer*self.writeCircleLen+self.writePos_peer)
        r = re_get(key)
        if not r:
            return True
        return False
    
    @gen.coroutine        
    def read_bytes(self):
        while True: 
            if not self.stall():
                break
            yield gen.sleep(miniSleep)
        ret = self.work()       
        self.refreshStatus()
        torRet(ret)
    def doClose(self):
        pass


class udpS():
    def __init__(self):
        self.readS = udpSR(self)
        self.writeS = udpSW(self)
        
    def ini(self,m):
        m = self.readS.ini(m)
        m = self.writeS.ini(m)
        return m

@gen.coroutine
def doRead(stream,readS):
    while True:
        try:
            d1 = yield readS.read_bytes()
        except:
            stream.close()
            return

        if len(d1)==0:
            continue
        try:
            yield stream.write(d1)
        except:
            readS.doClose()
            return

@gen.coroutine
def doWrite(stream,writeS):
    while True:
        try:
            while True:
                d1 = yield stream.read_bytes(1)
                if len(d1)!=0:
                    break
            data =  stream._read_buffer
            l = len(data)
            d2 = yield stream.read_bytes(l-1)
        except:
            writeS.doClose()
            return
        try:
            yield writeS.write(d1+d2)
        except:
            stream.close()
            return
        
@gen.coroutine
def accecptConn():
    for i in range(100):
        key = connKyeBase+str(i)
        r = re_get(key)
        if not r:
            continue
        re_set(key,'')
        m = json.loads(r)
        replyKey = m['replyKey']

        suc = 1
        ip = '127.0.0.1'
        try:
            stream = yield TCPClient().connect(ip, 8080)       
        except:
            suc = 0  
        m = {}
        m['status'] = suc
        if suc ==1:
            us = udpS()
            m = us.ini(m)
            writeS = us.readS
            readS = us.writeS
            IOLoop.instance().add_callback(functools.partial(doRead,stream,readS))
            IOLoop.instance().add_callback(functools.partial(doWrite,stream,writeS))            
        j = json.dumps(m)
        re_set(replyKey,j)
    

@gen.coroutine
def backG():
    while True:
        yield accecptConn()

 
if __name__ == "__main__":
    IOLoop.instance().add_callback(backG)
    IOLoop.instance().start()
    




