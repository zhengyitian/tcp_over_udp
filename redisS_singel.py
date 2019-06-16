import socket,select,json,base64
import os,sys
ll = sys.argv
g_port = range(9993,10093)
if len(ll)>1:
    g_port = int(ll[1])
pid = os.getpid()



recLen = 1024*16
import redis,random,time
conn = redis.Redis(host='localhost', port=6379, db=0)
import re as re3
packLimit = 700
def cut_text(text,lenth): 
    textArr = re3.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
    return textArr

def isTest():
    #return False
    s = random.randint(1,10)
    if s in [1,2,3]:
        return True
    return False
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
        
def send2(sock,re,mysign,addr):
    re['mysign'] = mysign
    if not isTest():
        sock.sendto(json.dumps(re), addr)   
checkInterval = 100
stayTime = 100
stayTime2 = 500
class reServer():
    def __init__(self,port):
        self.port = port
        self.lastCheckTime = time.time()
        self.seqM = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0',port))  
        
    def checkM(self):
        t = time.time()
        if t-self.lastCheckTime<checkInterval:
            return
        self.lastCheckTime = t
        for k,v in self.seqM.items():
            if v['touchTime']+stayTime2<t:
                del self.seqM[k]       
                continue
            if 'okTime' in v and v['okTime']+stayTime<t:
                del self.seqM[k]
serverMap = {}            
def doOne(sock):
    global serverMap
    server = serverMap[sock]
    try:
        data, addr = sock.recvfrom(recLen)
        server.checkM()
        m = json.loads(data)
        if isTest():
            raise Exception
    except:
        return
    opt = m['opt']
    mysign = m['mysign']    
    if opt=='set':
        seq = m['seq']
        key = m['key']    
        pack = m['pack']
        con = m['con']        
        size = m['size']
        if seq not in server.seqM :
            server.seqM[seq] = {}
            server.seqM[seq]['ok']=0
        server.seqM[seq]['touchTime'] = time.time()
        if seq in server.seqM and server.seqM[seq]['ok']==1:
            re = {'ret':1}
            send2(sock, re, mysign, addr)                 
            return

        if 'collect' not in server.seqM[seq]:
            server.seqM[seq]['collect'] = {}
        server.seqM[seq]['collect'][pack] =  base64.b64decode(con)
        if len(server.seqM[seq]['collect'].keys())!=size:
            re = {'ret':0}
            send2(sock, re, mysign, addr)                           
            return
        server.seqM[seq]['ok']=1
        server.seqM[seq]['okTime'] = time.time()
        ss = ''
        for i in range(size):
            ss += server.seqM[seq]['collect'][i]
        del  server.seqM[seq]['collect']
        re = {'ret':1}
        re_set(key,ss)
        send2(sock, re, mysign, addr)                       
        return    
    if opt=='get':
        seq = m['seq']
        key = m['key']    
        pack = m['pack']
        if seq not in server.seqM :
            server.seqM[seq] = {}
            server.seqM[seq]['key'] = key
            server.seqM[seq]['firstTime'] = True
        else:
            if server.seqM[seq]['key'] != key:
                ret = {'ret':-1}
                send2(sock, ret, mysign, addr)                                       
                return
        if server.seqM[seq]['firstTime']:
            server.seqM[seq]['firstTime']=False
            va = re_get(key)
            if va==None:
                va = ''

            l = cut_text(va,packLimit)
            server.seqM[seq]['con'] = l
            server.seqM[seq]['packNum'] = len(l)
        server.seqM[seq]['touchTime'] = time.time()
        if pack > server.seqM[seq]['packNum']-1:
            ret = {'ret':0,'packNum':server.seqM[seq]['packNum']}
            send2(sock, ret, mysign, addr)                                
            return
        ret = {'ret':1,'packNum':server.seqM[seq]['packNum'],'con':base64.b64encode(server.seqM[seq]['con'][pack])}
        send2(sock, ret, mysign, addr)                        
    
def main():
    global serverMap
    for i in g_port:
        s = reServer(i)
        serverMap[s.sock] = s
    while True :
        #print serverMap.keys()
        r = select.select(serverMap.keys(),[],[])
        for one in r[0]:
            doOne(one)
           
main()