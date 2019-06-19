from tornado import gen
import uuid,time,json,random
import select,socket,copy
from helpFunc import *

class statusServer():
    def __init__(self,port,toleranceTime,salt,isLocalTest=False):     
        self.lastTime = 0        
        self.ip = '0.0.0.0'
        self.port = port
        self.salt = salt
        self.serverStatus = {}
        self.clientStatus = {}
        self.lastUpdateTime = 0
        self.toleranceTime = toleranceTime
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('0.0.0.0',self.port))  
        self.isLocalTest = isLocalTest
        
    def isTest(self):
        if not self.isLocalTest:
            return False
        r = random.randint(1,10)
        if r < 2:
            return True
        
    @gen.coroutine
    def getClientStatus(self):
        while self.lastUpdateTime+self.toleranceTime<time.time():
            yield gen.sleep(miniSleep)
        torRet(copy.deepcopy(self.clientStatus))
        
    @gen.coroutine
    def setServerStatus(self,m):
        while self.lastUpdateTime+self.toleranceTime<time.time():
            yield gen.sleep(miniSleep)
        self.serverStatus = m    
        
    @gen.coroutine
    def doWork(self):
        while True:
            r = select.select([self.sock],[],[],0)
            if not r[0]:
                return 
            data, addr = self.sock.recvfrom(recLen)
            uuid ,ss = checkPackValid_server(data,self.salt)
            if not uuid or self.isTest():
                continue
            m = json.loads(ss)
            ti = m['time']
            if ti>lastTime:
                self.lastTime = ti
                self.clientStatus = m   
                self.lastUpdateTime = time.time()
            m = copy.deepcopy(self.serverStatus)
            m['time']=time.time()
            j = json.dumps(m)
            data = makePack_server(j, uuid, self.salt)
            if self.isTest():
                continue
            if self.isLocalTest:
                yield gen.sleep(random.randint(200,300)/1000.0)
                self.sock.sendto(data,addr)

