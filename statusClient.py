from tornado import gen
import uuid,time,json,random
import select,socket,copy
from helpFunc import *

class statusClient():
    def __init__(self,ip,port,inRoadNum,toleranceTime,salt):
        self.inRoadNum = inRoadNum        
        self.lastTime = 0        
        self.ip = ip
        self.port = port
        self.salt = salt
        self.serverStatus = {}
        self.clientStatus = {}
        self.lastUpdateTime = 0
        self.toleranceTime = toleranceTime
        
    @gen.coroutine
    def getServerStatus(self):
        while self.lastUpdateTime+self.toleranceTime<time.time():
            yield gen.sleep(miniSleep)
        torRet(copy.deepcopy(self.serverStatus))
        
    @gen.coroutine
    def setClientStatus(self,m):
        while self.lastUpdateTime+self.toleranceTime<time.time():
            yield gen.sleep(miniSleep)
        self.clientStatus = m
                
    def deal_rec(self,l):
        for s in l:
            j = s.recv(recLen)
            u = self.sockMap[s]['uuid']
            s2 = checkPackValid(j,u,salt)
            if not s2:
                continue                                
            s.close()
            del sockMap[s]
            m = json.loads(s2)
            ti = m['time']
            if ti>self.lastTime:
                self.lastTime = ti
                self.serverStatus = m   
                self.lastUpdateTime = time.time()
    
    def deal_timeout(self):
        for k ,v in  self.sockMap.items():
            ti = v['createTime']
            if ti+timeoutTime<time.time():
                k.close()
                del self.sockMap[k]
                
    def resend(self):
        n = self.inRoadNum - len(self.sockMap)
        for i in range(n):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            m = copy.deepcopy(self.clientStatus)
            m ['time']=time.time()
            j = json.dumps(m)
            uuid ,s2 = makePack(j,self.salt)
            s.sendto(s2, (self.ip, self.port))
            self.sockMap[s] = {'createTime':time.time(),'uuid':uuid}
            
    def doWork(self):
        while True:
            if len(self.sockMap)!=0:
                rr = select.select(self.sockMap.keys(),[],[],0)
                if rr[0]!=[]:
                    self.deal_rec(rr[0])
            self.deal_timeout()
            self.resend()
    
            
        