import socket,select,json,base64
import os,sys
ll = sys.argv
g_port = 9994
if len(ll)>1:
    g_port = int(ll[1])
pid = os.getpid()
os.system('mkdir pids_get/')
f = open('pids_get/'+str(pid),'w')
f.close()
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('0.0.0.0',g_port))
seqM = {}
recLen = 1024*16
import redis,random,time
conn = redis.Redis(host='localhost', port=6379, db=0)
packLimit=700
import re
def cut_text(text,lenth): 
    textArr = re.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
    return textArr

def isTest():
    return False
    s = random.randint(1,10)
    if s in [1,2,3]:
        return True
    return False

def re_set(k,s):
    global conn
    try:
        conn.set(k,s)
    except:
        conn = redis.Redis(host='localhost', port=6379, db=0)
        conn.set(k,s)
def re_get(k):
    global conn
    try:
        return conn.get(k)
    except:
        conn = redis.Redis(host='localhost', port=6379, db=0)
        return conn.get(k)        
    
lastCheckTime = time.time()
checkInterval = 100
stayTime = 100

def checkM():
    global lastCheckTime,seqM
    t = time.time()
    if t-lastCheckTime<checkInterval:
        return
    lastCheckTime = t
    for k,v in seqM.items():
        if v['touchTime']+stayTime<t:
            del seqM[k]
while True :
    try:
        data, addr = sock.recvfrom(recLen)
        checkM()
        m = json.loads(data)
        if isTest():
            raise Exception
    except:
        continue
    seq = m['seq']
    key = m['key']
    opt = m['opt']
    pack = m['pack']
    if seq not in seqM :
        seqM[seq] = {}
        seqM[seq]['key'] = key
        seqM[seq]['firstTime'] = True
    else:
        if seqM[seq]['key'] != key:
            ret = {'ret':-1}
            if not isTest():
                sock.sendto(json.dumps(ret), addr)   
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
        if not isTest():
            sock.sendto(json.dumps(ret), addr)    
        continue
    ret = {'ret':1,'packNum':seqM[seq]['packNum'],'con':base64.b64encode(seqM[seq]['con'][pack])}
    if not isTest():
        sock.sendto(json.dumps(ret), addr)    
  