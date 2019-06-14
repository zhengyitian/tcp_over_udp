import socket,select,json,base64
import os,sys
ll = sys.argv
g_port = 9993
if len(ll)>1:
    g_port = int(ll[1])
pid = os.getpid()
os.system('mkdir pids/')
f = open('pids/'+str(pid),'w')
f.close()
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('0.0.0.0',g_port))
seqM = {}
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
    return False
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
        
lastCheckTime = time.time()
checkInterval = 100
stayTime = 100
stayTime2 = 500
def checkM():
    global lastCheckTime,seqM
    t = time.time()
    if t-lastCheckTime<checkInterval:
        return
    lastCheckTime = t
    for k,v in seqM.items():
        if v['touchTime']+stayTime2<t:
            del seqM[k]       
            continue
        if 'okTime' in v and v['okTime']+stayTime<t:
            del seqM[k]
            
            
def send2(sock,re,mysign,addr):
    re['mysign'] = mysign
    if not isTest():
        sock.sendto(json.dumps(re), addr)    
while True :
    try:
        data, addr = sock.recvfrom(recLen)
        checkM()
        m = json.loads(data)
        if isTest():
            raise Exception
    except:
        continue
    opt = m['opt']
    mysign = m['mysign']    
    if opt=='set':
        seq = m['seq']
        key = m['key']    
        pack = m['pack']
        con = m['con']        
        size = m['size']
        if seq not in seqM :
            seqM[seq] = {}
            seqM[seq]['ok']=0
        seqM[seq]['touchTime'] = time.time()
        if seq in seqM and seqM[seq]['ok']==1:
            re = {'ret':1}
            send2(sock, re, mysign, addr)                 
            continue
        
        if 'collect' not in seqM[seq]:
            seqM[seq]['collect'] = {}
        seqM[seq]['collect'][pack] =  base64.b64decode(con)
        if len(seqM[seq]['collect'].keys())!=size:
            re = {'ret':0}
            send2(sock, re, mysign, addr)                           
            continue
        seqM[seq]['ok']=1
        seqM[seq]['okTime'] = time.time()
        ss = ''
        for i in range(size):
            ss += seqM[seq]['collect'][i]
        del  seqM[seq]['collect']
        re = {'ret':1}
        re_set(key,ss)
        send2(sock, re, mysign, addr)                       
        continue    
    if opt=='get':
        seq = m['seq']
        key = m['key']    
        pack = m['pack']
        if seq not in seqM :
            seqM[seq] = {}
            seqM[seq]['key'] = key
            seqM[seq]['firstTime'] = True
        else:
            if seqM[seq]['key'] != key:
                ret = {'ret':-1}
                send2(sock, ret, mysign, addr)                                       
                continue
        if seqM[seq]['firstTime']:
            seqM[seq]['firstTime']=False
            va = re_get(key)
            if va==None:
                va = ''
            l = cut_text(va,packLimit)
            seqM[seq]['con'] = l
            seqM[seq]['packNum'] = len(l)
        seqM[seq]['touchTime'] = time.time()
        if pack > seqM[seq]['packNum']-1:
            ret = {'ret':0,'packNum':seqM[seq]['packNum']}
            send2(sock, ret, mysign, addr)                                
            continue
        ret = {'ret':1,'packNum':seqM[seq]['packNum'],'con':base64.b64encode(seqM[seq]['con'][pack])}
        send2(sock, ret, mysign, addr)                       
       
