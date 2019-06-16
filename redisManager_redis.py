import redis
from tornado import gen
import uuid,time,json,random
miniSleep = 0.01
def torRet(r):
    e = StopIteration()
    e.value = r
    raise e  
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
            if v['createTime'] +random.randint(100,200)*0.2/100.0>time.time():
                opt,k2,v2 = v['task']
                ret = ''
                if opt == 'get':
                    ret = re_get(k2)
                    if not ret:
                        ret = ''
                if opt == 'set':
                    ret = re_set(k2, v2)
                    ret = None
                self.outputMap[k] = ret
                del self.inputMap[k]
                
