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
from redisMan import g_man
miniSleep = 0.01

firstKey = 'firstKey'
connKyeBase = 'connKyeBase'
connReplyKey = 'connReplyKey'
isStart = True
connMap = {}
for i in range(100):
    connMap[i] = 0

def getAvailConn():
    global connMap
    for k,v in connMap.items():
        if v==0:
            connMap[k] = 1
            return k

def torRet(r):
    e = StopIteration()
    e.value = r
    raise e  



g_manager = g_man

class udpSW():
    def __init__(self,parent):
        self.parent = parent
        self.wirteKeyBase = ''
        self.writeCircle = 0  # 0 ir 1
        self.writeCircle_peer = 0
        self.writeCircleLen = 0
        self.writePos = 0
        self.writePos_peer = 0
        self.statusKey = ''
        self.close = False
        self.writeTaskIds = {}
        self.seq = 0
        self.seqLimit = 10000
  

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
                seq = m['seq']
                if seq ==self.seq or (seq <self.seq and seq +1==self.seq) or (seq >self.seq and seq +1 == self.seq+self.seqLimit): 
                    self.writeCircle_peer = m['writeCircle_peer']
                    self.writePos_peer = m['writePos_peer']
                    
    def deal_close(self):
        pass
    @gen.coroutine        
    def write(self,s):
        self.work()  
        while True:
            if not self.stall():
                break
            yield gen.sleep(miniSleep)
            self.work()  
        if self.close:
            self.deal_close()
            raise Exception
        key = self.wirteKeyBase+str(self.writeCircle*self.writeCircleLen+self.writePos)
        id = yield g_manager.add(['set',key,s])
        self.writeTaskIds[id]=0
        id = yield g_manager.add(['get',self.statusKey,1])
        self.writeTaskIds[id]=0
        if self.writePos == self.writeCircleLen-1:
            self.seq += 1
            if self.seq == self.seqLimit:
                self.seq = 0
            self.writePos = 0
            if self.writeCircle == 0:
                self.writeCircle = 1
            else:
                self.writeCircle = 0  
        else:
            self.writePos += 1
    def doClose(self):
        return
class udpSR():
    def __init__(self,parent):
        self.parent = parent
        self.readAhead = 10
        self.readCircleLen = 0 #like 200
        self.hasGot = {}
        self.hasSend = {}
        self.buff = {}
        self.taskIds = {}
        self.isSending = {}
        self.readKeyBase = ''
        self.clientReadStatusKey = ''
        self.close = False
        self.packSize = {}
        self.clientPos = 0
        self.clientSendStatusKey = ''
        self.serverPos = 0
   
    def doClose(self):
        pass
    def clearBuffer(self):
        ss = ''
        while self.clientPos in self.hasGot:
            ss += self.buff[self.clientPos]
            del self.buff[self.clientPos]
            del self.hasGot[self.clientPos]
            del self.hasSend[self.clientPos]
            if self.clientPos in self.isSending:
                del self.isSending[self.clientPos]
            self.clientPos = self.circleAdd(self.clientPos,1)
        return ss
    
    @gen.coroutine    
    def read_bytes(self):
        yield self.work()
        while True:
            if not self.stall():
                break
            yield gen.sleep(miniSleep)
            yield self.work()
        ret = self.clearBuffer()
        m = {'clientPos':self.clientPos}
        task = ['set',self.clientSendStatusKey,json.dumps(m)]
        id = yield g_manager.add(task)     
        self.taskIds[id] = {'type':'setStatus'}
        torRet(ret)
        
    def circleRange(self,a,b):  # return [,)  same as range
        temp = a
        ret = []
        while True:
            if not self.circleBig(b,temp):
                break
            ret.append(temp)
            temp = self.circleAdd(temp,1)
        return ret
    def circleMax(self,l):
        ret = None
        k = l.keys()
        for i in k:
            if ret==None:
                ret = i
            if self.circleBig(i,ret):
                ret = i
        return ret

    def circleBig(self,a,b):
        if a==b:
            return False
        if a>b and (a-b)<(self.readCircleLen/2):
            return True
        if a<b and (b-a)>(self.readCircleLen/2):
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
     
    def stall(self):
        if self.close:
            return False
        if self.clientPos in self.hasGot:
            return False
        return True

    @gen.coroutine    
    def work(self):
        pack_to_send = []
        keys = self.taskIds.keys()
        for k in keys:
            if k in g_manager.outputMap:
                r = g_manager.get(k)
                con = self.taskIds[k]
                if con['type']=='readPack':
                    pack = con['pack']
                    if pack not in self.isSending:
                        continue
                    del self.isSending[pack]
                    if r:
                        self.hasGot[pack] = 1
                        self.buff[pack] = r         
                if con['type']=='readStatus' and  r!= '':
                    mm = json.loads(r)
                    if mm['close'] == 1:
                        self.deal_close()
                        return
                    serverPos = mm['serverPos']
                    clientPos = mm['clientPos']
                    size = mm['packSize']
                    l = self.circleRange(self.clientPos,serverPos)
                    for i in l:
                        self.packSize[i] = size[str(i)]
                    if self.circleBig(serverPos,self.serverPos):
                        self.serverPos = serverPos
                del self.taskIds[k]
        ma = self.serverPos
        tt = self.circleMax(self.hasGot)
        if tt and self.circleBig(tt,self.serverPos):
            ma = tt
        l = self.circleRange(self.clientPos,ma)
        for one in l:
            if one not in self.isSending and one not in pack_to_send:
                pack_to_send.append(one)
        ma2 = self.circleAdd(ma,self.readAhead)
        l = self.circleRange(ma,ma2)
        for one in l:
            if one not in self.hasSend and one not in pack_to_send:
                pack_to_send.append(one)
                
        for one in pack_to_send:
            yield self.sendAll(one)
          
            
        if not pack_to_send and not self.isSending:
            st = self.clientPos
            co = 0
            ll = []
            while co<self.readAhead:
                if st not in self.hasGot:
                    co +=1
                    yield self.sendAll(st)
                    st = self.circleAdd(st,1)
                    
    @gen.coroutine    
    def sendAll(self,one):
        size = 1
        if one in self.packSize:
            size = self.packSize[one]
            
        task = ['get',self.readKeyBase+str(one),size]
        id = yield g_manager.add(task)
        self.taskIds[id] = {'type':'readPack','pack':one}
        self.isSending[one] = 1
        self.hasSend[one] = 1
        task = ['get',self.clientReadStatusKey,2]
        id = yield g_manager.add(task)
        self.taskIds[id] = {'type':'readStatus'}
        
    def deal_close(self):
        pass

class udpS():
    def __init__(self,num):
        self.readS = udpSR(self)
        self.writeS = udpSW(self)
        self.num = num
    @gen.coroutine        
    def connect(self):
        global connMap
        key = connKyeBase+str(self.num)
        st = str(uuid.uuid1())
        m = {'replyKey':st}
        j = json.dumps(m)
        yield g_manager.synFunc(['set',key,j])
        while True:
            r = yield  g_manager.synFunc(['get',st,1])
            if r:
                break
        connMap[self.num]=0
        m = json.loads(r)
        if m['status'] != 1:
            torRet(False)
        self.writeS.wirteKeyBase = m['wirteKeyBase']
        self.writeS.writeCircleLen = m['writeCircleLen']
        self.writeS.statusKey = m['statusKey']
        
        self.readS.readCircleLen = m['readCircleLen']
        self.readS.readKeyBase = m['readKeyBase']
        self.readS.clientReadStatusKey = m['clientReadStatusKey']
        self.readS.clientSendStatusKey = m['clientSendStatusKey']       
        torRet(True)        
        



class EchoServer(TCPServer):  
    @gen.coroutine
    def handle_stream(self, stream, address):
        while True:
            r = getAvailConn()
            if r != None:
                break
            yield gen.sleep(miniSleep)
        us = udpS(r)
        re = yield us.connect()
        if not re:
            stream.close()
            return
        print 'client ok'
        readS = us.readS
        writeS = us.writeS
        IOLoop.instance().add_callback(functools.partial(self.doRead,stream,readS))
        #IOLoop.instance().add_callback(functools.partial(self.doWrite,stream,writeS))
        yield self.doWrite(stream, writeS)
            
    @gen.coroutine
    def doRead(self,stream,readS):
        while True:
            d1 = yield readS.read_bytes()
            if not d1:
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
    def doWrite(self, stream,writeS):
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

def start_server():
    server = EchoServer()
    server.listen(9999,'0.0.0.0')



if __name__ == "__main__":
    start_server()
    managerTimer  = PeriodicCallback(g_manager.doWork,10)
    managerTimer.start()    
    IOLoop.instance().start()
