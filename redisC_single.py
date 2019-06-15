import threading
import Queue,time
import base64,copy
import json,uuid
import random,os,sys
import socket,select
import re as re3
import redis

waitTime = 0.7
packLimit=700
sockNum = 1000

g_port = [9994,9994]
g_ip = '144.202.17.72'    
#g_ip = '0.0.0.0'
    
workLimit = 3
recLen = 16*1024
testId = -1
sleepTime = 0.05
sleepTime2 = 0.05
minSleep = 0.01
sockMap = {}
taskMap = {}
maxTask = 100
def cut_text(text,lenth): 
    textArr = re3.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
    return textArr

def get_port():
    return random.randint(*g_port)



class task_g():
    def __init__ (self):
        self.m = {}
        self.co = 0
        self.speed = 0
        self.markTime = time.time()
    def getPri(self,ti):
        t = time.time()
        return int ((t-ti)/0.3)
    def add(self,con):
        id = str(uuid.uuid1())
        con['id'] = id
        con['createTime'] = time.time()
        con['workCount'] = 0
        con['status'] = 0
        self.m[id]=con
        return id

 
    def getTaskMinLoad(self,taskId):
        global taskMap
        minLoad = workLimit+1
        minLoadList = []
        for one in taskMap[taskId]['ids']:
            if self.m[one]['workCount']<minLoad and self.m[one]['status']==0:
                minLoadList = [one]
                minLoad = self.m[one]['workCount']
            if self.m[one]['workCount']==minLoad and self.m[one]['status']==0:
                minLoadList.append(one)  
        l = len(minLoadList)
        pick = minLoadList[random.randint(0,l-1)]    
        return pick
    
    def pickTask(self):   
        global taskMap
        maxPri = 0
        maxPriList = []

        for k,v in taskMap.items():
            if v['minLoad']<workLimit:
                id = k
                ti = v['createTime']
                pri = self.getPri(ti)
                if pri>maxPri:
                    maxPriList = [id,]
                    maxPri = pri
                if pri==maxPri:
                    maxPriList.append(id)
        if not maxPriList:
            return
        minLoad = workLimit+1
        minLoadList = []
        for one in maxPriList:
            if taskMap[one]['minLoad']<minLoad:
                minLoadList = [one]
                minLoad = taskMap[one]['minLoad']
            if taskMap[one]['minLoad']==minLoad:
                minLoadList.append(one)
        l = len(minLoadList)
        pick = minLoadList[random.randint(0,l-1)]
        pick = self.getTaskMinLoad(pick)
        v = self.m[pick]
        ret = copy.deepcopy(v)
        self.m[pick]['workCount'] = self.m[pick]['workCount']+1
        self.refreshTask(v['taskId'])
       
        return ret     
            
    def findTaskId(self,id):
        global taskMap
        for k,v in taskMap.items():
            if id in v['ids']:
                return k
    def refreshTask(self,taskId):
        minLoadList = []
        for one in taskMap[taskId]['ids']:
            if self.m[one]['status'] == 0:
                minLoadList.append(self.m[one]['workCount'])
        taskMap[taskId]['minLoad'] = min(minLoadList)        
    def deal_timeout(self,m):
        k = m['id']
        if k not in self.m:
            return        
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
        taskId = self.findTaskId(k)
        self.refreshTask(taskId)
        
    def deal_get_back(self,m,j):
        global taskMap
        k = m['id']
        if k not in self.m:
            return
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
        self.m[k]['status'] = 1
        ret = j['ret']
        if ret == -1:
            print 'error'
            return
        packNum = j['packNum']
        taskId = m['taskId']
        ll = len(taskMap[taskId]['ids'])
        if packNum > ll:
            for i in range(packNum-ll):
                con = {}
                for sss in ['key','ip','port','opt','seq']:
                    con[sss] = m[sss]
                con['pack'] = packNum-1-i          
                g_task.add(con)       
        elif packNum<ll:
            tempL = []
            for i in range(ll-packNum):
                tempL.append(packNum+i)
            for k in taskMap[taskId]['ids']:
                v = self.m[k]
                if v['pack'] in tempL:
                    self.m[k]['status'] = 1            
        if ret == 1:
            con = j['con']
            id = m['id']
            self.m[id]['con'] = base64.b64decode(con)
            co = 0
            taskId = m['taskId']
            for one in taskMap[taskId]['ids']:
                oo = self.m[one]
                if oo['status']==1:
                    co += 1     
            ll = len(taskMap[taskId]['ids'])                
            if co == ll:
                tempM = {}
                for i in taskMap[taskId]['ids']:
                    one = self.m[i]                  
                    tempM[one['pack']] = one.get('con','')
                ss = ''
                for i in range(ll):
                    ss += tempM[i]
                for one in taskMap[taskId]['ids']:
                    del self.m[one]
                tt = taskMap[taskId]['createTime']
                del taskMap[taskId]                
                self.co += 1
                if self.co %100 == 0:
                    self.speed = time.time()-self.markTime
                    self.markTime = time.time()
                print self.speed,self.co,time.time(), 'done!!',len(ss),ss[:10],tt

    def deal_set_back(self,m,j):
        global taskMap
        k = m['id']
        if k not in self.m:
            return
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
        self.m[k]['status'] = 1
        co = 0
        taskId = m['taskId']
        for one in taskMap[taskId]['ids']:
            oo = self.m[one]
            if oo['status']==1:
                co += 1            
            
        if co == m['size']:
            for one in taskMap[taskId]['ids']:
                del self.m[one]
            tt = taskMap[taskId]['createTime']
            del taskMap[taskId]
            self.co += 1
            if self.co %100 == 0:
                self.speed = time.time()-self.markTime
                self.markTime = time.time()
            print self.speed,self.co,time.time(), 'done!!',tt
        else:
            self.refreshTask(taskId)

    def deal_back(self,m,back):
        opt = m['opt']
        if opt == 'set':
            return self.deal_set_back(m, back)
        if opt == 'get':
            return self.deal_get_back(m, back)        

g_task = task_g()   
def assign_task(m):
    global sockMap
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ip = m['ip']
    port = m['port']    
    opt = m['opt']
    mysign = str(uuid.uuid1())
    if opt == 'set':
        con = {}
        for sss in ['opt','pack','key','seq','size','con']:
            con[sss] = m[sss]        
    if opt == 'get':
        con = {}
        for sss in ['opt','pack','key','seq']:
            con[sss] = m[sss]        
    con['mysign'] = mysign
    m['mysign'] = mysign
    ra = json.dumps(con)   
    sockMap[s] = {}
    sockMap[s]['createTime'] = time.time()
    sockMap[s]['con'] = m
    s.sendto(ra, (ip, port))
  
def splitTask(taskId):
    global taskMap
    taskMap[taskId]['createTime'] = time.time()
    opt,k,v = taskMap[taskId]['command']
    taskMap[taskId]['minLoad'] = 0
    taskMap[taskId]['ids'] = []
    if opt=='get':
        seq = str(uuid.uuid1())
        for i in range(v):
            con = {'key':k,'ip':g_ip,'port':get_port(),'opt':'get','pack':i,'seq':seq,'taskId':taskId}
            id = g_task.add(con)
            taskMap[taskId]['ids'].append(id)
    if opt=='set':
        seq = str(uuid.uuid1())
        l = cut_text(v,packLimit)
        le = len(l)     
        for i in range(le):
            con = {'taskId':taskId,'key':k,'ip':g_ip,'port':get_port(),'opt':'set','size':le,'pack':i,'con':base64.b64encode(l[i]),'seq':seq}
            id = g_task.add(con)
            taskMap[taskId]['ids'].append(id)
        
                
def getTask():
    global taskMap
    while len(taskMap)<maxTask:
        taskId = str(uuid.uuid1())
        taskMap[taskId] = {}
        taskMap[taskId]['command'] = ['get','a',50]
        splitTask(taskId)
        
def deal_sock(l):
    global sockMap
    for s in l:
        m = sockMap[s]['con']
        mysign = m['mysign']
        try:
            j = json.loads(s.recv(recLen))   
        except:
            del sockMap[s]
            s.close() 
            print 'error2'
            return 
        if j['mysign'] != mysign:
            print 'colli'
            return
        g_task.deal_back(m,j)
        del sockMap[s]
        s.close()    

def deal_timeout_socks():
    global sockMap
    for s,v in sockMap.items():
        ti = v['createTime']
        if time.time()-ti>waitTime:
            m = v['con']
            g_task.deal_timeout(m)   
            del sockMap[s]
            s.close()    

def main(myid):
    global g_task,sockMap
    while True:
        if myid ==testId:
            print time.time(),'sign1'
        while len(sockMap)<sockNum:
            m = g_task.pickTask()
            if m == None:
                break
            assign_task(m)
        whatReady = select.select(sockMap.keys(), [], [],0.01)   
        if whatReady[0] != []:
            deal_sock(whatReady[0])
        deal_timeout_socks()
        getTask()

main(11)