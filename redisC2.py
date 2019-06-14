import threading
import Queue,time
import base64,copy
import json,uuid
import random
sleepTime = 0.05
sleepTime2 = 0.05
import socket,select
q = Queue.Queue()
q2 = Queue.Queue()
import re 
waitTime = 0.7
packLimit=700
threadNum = 150

g_port = 9993
g_ip = '144.202.17.72'    
    
workLimit = 3
recLen = 16*1024
testId = -1


class task_g():
    def __init__ (self):
        self.m = {}
        self.lock = threading.Lock()
        self.seqM = {}
    def lockWrapper(func):
        def wrapper(self,*arg,**karg):
            self.lock.acquire()       
            ret = func(self,*arg,**karg)
            self.lock.release()                               
            return ret    
        return wrapper   
    
    @lockWrapper
    def add(self,con):
        id = str(uuid.uuid1())
        con['id'] = id
        con['createTime'] = time.time()
        con['workCount'] = 0
        con['status'] = 0
        self.m[id]=con
        l = len(self.m.keys())

    
    def add2(self,con):
        id = str(uuid.uuid1())
        con['id'] = id
        con['createTime'] = time.time()
        con['workCount'] = 0
        con['status'] = 0
        self.m[id]=con
        l = len(self.m.keys())

    def delete(self,id):
        self.lock.acquire()
        if id in self.m:
            del self.m[id]
        self.lock.release()
        
    @lockWrapper
    def check_id(self,id):
        ret = None
        if id not in self.m:
            ret = [-1,{}]
        else:
            st = self.m[id]['status']
            if st == 1:
                ret = [1,copy.deepcopy(self.m[id])]
                del self.m[id]
            else:
                ret = [0,{}]
        return  ret
    
    @lockWrapper
    def pickTask(self):     
        sTime = time.time()
        l = len(self.m.keys())
        tt = []        
        v = self.m.values()
        for i in v:
            if i['status']!=1 and i['workCount']<workLimit:
                tt.append(i)
        l = [one['workCount'] for one in tt]
        if len(l)==0:
            return 
        mi = min(l)
        temp = []
        for one in tt:
            if one['workCount']==mi:
                temp.append(one['createTime'])
        mi2 = min(temp)  
        for k,v in self.m.items():
            if v['workCount'] == mi and v['createTime']==mi2:
                ret = copy.deepcopy(v)
                self.m[k]['workCount'] = self.m[k]['workCount']+1
                #print time.time()-sTime,'xxoo'

                return ret
            
    @lockWrapper                
    def deal_timeout(self,m):
        k = m['id']
        if k not in self.m:
            return        
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
    def deal_get_back(self,m,j):
        k = m['id']
        if k not in self.m:
            return
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
        #m = {'ip':g_ip,'port':9993,'opt':'set','size':le,'pack':i,'con':base64.b64encode(l[i]),'seq':seq}
        self.m[k]['status'] = 1
        ret = j['ret']
        if ret == -1:
            self.m = {}
            q2.put(None)            
            return 
        packNum = j['packNum']
        k = self.m.keys()
        ll = len(k)
        if packNum > ll:
            for i in range(packNum-ll):
                con = {}
                for sss in ['key','ip','port','opt','seq']:
                    con[sss] = m[sss]
                con['pack'] = packNum-1-i          
                g_task.add2(con)       
        elif packNum<ll:
            tempL = []
            for i in range(ll-packNum):
                tempL.append(packNum+i)
            for k,v in self.m.items():
                if v['pack'] in tempL:
                    self.m[k]['status'] = 1            
        if ret == 1:
            con = j['con']
            
            #{'ret':1,'packNum':seqM[seq]['packNum'],'con'
            id = m['id']
            self.m[id]['con'] = base64.b64decode(con)
            co = 0
            for one in self.m.values():
                if one['status']==1:
                    co += 1
            k = self.m.keys()
            ll = len(k)                    
            if co == ll:
                tempM = {}
                v = self.m.values()
                for one in v:                    
                    tempM[one['pack']] = one.get('con','')
                ss = ''
                self.m = {}
                for i in range(ll):
                    ss += tempM[i]
                q2.put(ss)    
    def deal_set_back(self,m,j):
        k = m['id']
        if k not in self.m:
            return
        self.m[k]['workCount'] = self.m[k]['workCount']-1   

        #m = {'ip':g_ip,'port':9993,'opt':'set','size':le,'pack':i,'con':base64.b64encode(l[i]),'seq':seq}
        self.m[k]['status'] = 1
        #print 'pack',m['pack']
        co = 0
        for one in self.m.values():
            if one['status']==1:
                co += 1
            
        if co == m['size']:
            self.m = {}
            q2.put('ok')
            
    @lockWrapper
    def deal_back(self,m,back):
        opt = m['opt']
        if opt == 'set':
            return self.deal_set_back(m, back)
        if opt == 'get':
            return self.deal_get_back(m, back)        
    
    
    @lockWrapper
    def check_sta(self,m):
        k = m['id']
        if k not in self.m:
            return True            
        if self.m[k]['status'] == 1:
            return True
        return False
g_task = task_g()   

def thr_func(myid):
    global g_task

    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        mysign = random.randint(1,999)
        mysign = str(uuid.uuid1())

        if myid ==testId:
            print time.time(),'sign1'
        m = g_task.pickTask()
        if m == None:
            time.sleep(sleepTime)
            if myid ==testId:
                print time.time(),'sign2'            
            continue
        #print 'sign1',myid,m['pack']
        ip = m['ip']
        port = m['port']    
        opt = m['opt']
        
        if opt == 'set':
            con = {}
            for sss in ['opt','pack','key','seq','size','con']:
                con[sss] = m[sss]        

        if opt == 'get':
            con = {}
            for sss in ['opt','pack','key','seq']:
                con[sss] = m[sss]        
        con['mysign'] = mysign
        ra = json.dumps(con)   
        sTime = time.time()
        s.sendto(ra, (ip, port))
        retSign = False
        if myid ==testId:
            print time.time(),'sign3'        
        while True:
            whatReady = select.select([s], [], [], sleepTime2)   
            if g_task.check_sta(m):
                retSign = True
                s.close()
                break            
            if whatReady[0] == []: # Timeout
                if time.time()-sTime>waitTime:
                    s.close()
                    g_task.deal_timeout(m)   
                    retSign = True
                    break     
                else:
                    continue
            else:
                j = json.loads(s.recv(recLen))                
                if j['mysign'] != mysign:
                    print 'colli'
                    continue                  
                s.close()
                g_task.deal_back(m,j )                                
                break
        if myid ==testId:
            print time.time(),'sign4'            
        if retSign:
            continue      

        
        
thrList = []        
for i in range(threadNum):
    t = threading.Thread(target=thr_func,args=(i,))
    thrList.append(t)
    t.setDaemon(True)
    t.start()

    
def cut_text(text,lenth): 
    textArr = re.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
    return textArr


def func():
    while True:
        while q.empty():
            time.sleep(0.01)
        opt,k,v =  q.get()
        if opt=='get':
            seq = str(uuid.uuid1())
            g_task.lock.acquire()
            for i in range(v):
                #print 'start add',time.time()
                con = {'key':k,'ip':g_ip,'port':g_port,'opt':'get','pack':i,'seq':seq}
                g_task.add2(con)
                #print 'end add',time.time()
            g_task.lock.release()        
        if opt=='set':
            seq = str(uuid.uuid1())
            l = cut_text(v,packLimit)
            le = len(l)     
            g_task.lock.acquire()
            for i in range(le):
                #print 'start add',time.time()
                con = {'key':k,'ip':g_ip,'port':g_port,'opt':'set','size':le,'pack':i,'con':base64.b64encode(l[i]),'seq':seq}
                g_task.add2(con)
                #print 'end add',time.time()
            g_task.lock.release()

t = threading .Thread(target=func)   
t.start()

while True:
    print time.time(),'send'    
 
    #q.put(('set','a','s'*100*1000))
    q.put(('get','a',100))
    while q2.empty():
        time.sleep(0.01)
    s = q2.get()
    print time.time(),len(s),s[:10]
