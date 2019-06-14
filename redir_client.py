from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from concurrent.futures import ThreadPoolExecutor
innerThreadPool = ThreadPoolExecutor(100)
import threading,uuid,copy,json,select,socket
conn_timeout = 2
sleepTime = 0.01
waitTime = 0.7
threadNum = 100
recLen = 10240
workLimit = 10
packLimit = 700
import re 
def cut_text(text,lenth): 
    textArr = re.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 

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
        con['key'] = id
        con['createTime'] = time.time()
        con['workCount'] = 0
        con['status'] = 0
        self.m[id]=con
        return id
   
        
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
        v = self.m.values()
        l = [one['createTime'] for one in v]
        if len(l)==0:
            return 
        mi = min(l)
        temp = []
        for one in v:
            if one['createTime']==mi:
                temp.append(one['workCount'])
        mi2 = min(temp)  
        if mi2 >= workLimit:
            return
        
        for k,v in self.m.items():
            if v['createTime'] == mi and v['workCount']==mi2:
                if v['status']==1:
                    continue
                ret = copy.deepcopy(v)
                self.m[k]['workCount'] = self.m[k]['workCount']+1
                return ret
            
    @lockWrapper                
    def deal_timeout(self,m):
        k = m['key']
        if k not in self.m:
            return        
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
    
    def deal_connec_back(self,m,back):
        k = m['key']
        if k not in self.m:
            return
        self.m[k]['workCount'] = self.m[k]['workCount']-1   
        j = json.loads(back)
        if j['ret']==0 or j['ret']==1:
            self.m[k]['port1'] = j['port1']
            self.m[k]['port2'] = j['port2']
            self.m[k]['status'] = 1
            self.m[k]['connStat'] = 1
            return
        self.m[k]['status'] = 1
        self.m[k]['connStat'] = -1                
    @lockWrapper
    def deal_back(self,m,back):
        opt = m['opt']
        if opt == 'connect':
            return self.deal_connec_back(m, back)
        pass
    @lockWrapper
    def read(self,seq):
        if seq not in self.seqM:
            return -1,''
        if self.seqM[seq]['readClose']:
            return -1,''
        s = self.seqM[seq]['readBuffer']
        self.seqM[seq]['readBuffer'] = ''
        return 1,s
    
    
    @gen.coroutine        
    def write2(self,seq,s):   
        ret = 0
        self.lock.acquire()
        if seq not in self.seqM:
            e = StopIteration()
            e.value = -1
            self.lock.release()            
            raise e      
        co = self.seqM[seq]['count']
        maxPack = self.seqM[seq]['maxPack']
        if co >10000:
            e = StopIteration()
            e.value = 0
            self.lock.release()            
            raise e    
        for k,v in self.m.items():
            if v['opt'] == 'write' and v['pack']== maxPack:
                if v['hasSent']:
                    
        
        
        
    @gen.coroutine        
    def write(self,seq,s):        
        l = cut_text(s,packLimit)
        for one in l:
            s = yield self.write2(seq, one)
            if s == -1:
                raise Exception
            if s == 0:
                yield gen.sleep(sleepTime)
                
            while len(self.packM.keys())>10000:
                if self.closeSelf:
                    raise Exception                        
                yield gen.sleep(0.01)        
        if seq not in self.seqM:
            raise Exception
        while True:
            
        if self.seqM[seq]['writeClose']:
            e = StopIteration()
            e.value = -1
            raise e                    
        
        pass
    
g_task = task_g()   

def makeRaw(m):
    opt = m['opt']
    ret = {}
    if opt == 'connect':
        ret = {'opt':'connect','seq':m['seq']}
    return json.dumps(ret)
        
def thr_func():
    while True:
        m = g_task.pickTask()
        if m == None:
            time.sleep(0.1)
            continue
        ip = m['ip']
        port = m['port']    
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ra = makeRaw(m)
        s.sendto(ra, (ip, port))
        whatReady = select.select([s], [], [], waitTime)
        if whatReady[0] == []: # Timeout
            s.close()
            g_task.deal_timeout(m)    
            continue     
        g_task.deal_back(m, s.recv(recLen))
        s.close()
        continue
        
thrList = []        
for i in range(threadNum):
    t = threading.Thread(target=thr_func)
    thrList.append(t)
    t.setDaemon(True)
    t.start()
    print i
    
def conn_func(ip,po):
    seq = str(uuid.uuid1())
    m = {'ip':ip,'port':po,'seq':seq,'opt':'connect'}

    id = g_task.add( m)
    sTime = time.time()
    #while time.time()-sTime<conn_timeout:
    while True:
        time.sleep(sleepTime)
        r,rm = g_task.check_id(id)
        if r==1:
            return rm


class udpS():
    def __init__(self,ip,po):
        self.buff = ''
        self.ip = ip
        self.po = po
        self.seq = ''

    @gen.coroutine        
    def connect(self):
        result = yield innerThreadPool.submit(conn_func, self.ip,self.po)
        e = StopIteration()
        e.value = result
        raise e                

    @gen.coroutine    
    def read_bytes(self):
        while True:
            si,s = g_task.read(self.seq)
            if  si ==0:
                yield gen.sleep(sleepTime)
                continue
            elif si==1:
                e = StopIteration()
                e.value = s
                raise e     
            elif si==-1:
                raise Exception

    @gen.coroutine        
    def write(self,s):
        si = yield g_task.write(self.seq, s)
        if si == -1:
            raise Exception
     



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


@gen.coroutine
def tt():

    result = yield innerThreadPool.submit(sl, ('s',))   
    yield gen.sleep(5)
    e = StopIteration()
    e.value = 11
    raise e        

class EchoServer(TCPServer): 
    clients = set()    
    @gen.coroutine
    def handle_stream(self, stream, address):
        ip, fileno = address
        print("Incoming connection from " + ip)
        EchoServer.clients.add(address)
        ip = '0.0.0.0'  
        po = 9993        
        #streamCli = yield TCPClient().connect(ip, 8080)       
        us = udpS(ip,po)
        re = yield us.connect()
        stat = re['connStat']
        if stat == -1:
            stream.close()
            return
        
        p1 = re['port1']
        p2 = re['port2']
        seq = re['seq']
        m = {'opt':'read','ip':ip,'port':p1,'seq':seq,'pack':0,'maxPack':0}
        id = g_task.add(m)

        print 'client ok'
        udpS.seq = seq
        IOLoop.instance().add_callback(functools.partial(run_client,stream,streamCli))

        while True:
            try:
                yield self.echo(stream,streamCli)
            except StreamClosedError:
                print("Client " + str(address) + " left.")
                EchoServer.clients.remove(address)
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
    IOLoop.instance().start()
