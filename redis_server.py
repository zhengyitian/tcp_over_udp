import socket,select,json,base64

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('0.0.0.0',9993))
seqM = {}
recLen = 1024*16
import redis,random,time
conn = redis.Redis(host='localhost', port=6379, db=0)
def isTest():
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
        if 'okTime' in v and v['okTime']+stayTime<t:
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
    con = m['con']
    size = m['size']
    if seq not in seqM :
        seqM[seq] = {}
        seqM[seq]['ok']=0
    if seq in seqM and seqM[seq]['ok']==1:
        re = {'ret':1}
        if not isTest():
            sock.sendto(json.dumps(re), addr)          
        continue
    
    if 'collect' not in seqM[seq]:
        seqM[seq]['collect'] = {}
    seqM[seq]['collect'][pack] =  base64.b64decode(con)
    if len(seqM[seq]['collect'].keys())!=size:
        re = {'ret':0}
        if not isTest():
            sock.sendto(json.dumps(re), addr)  
        continue
    seqM[seq]['ok']=1
    seqM[seq]['okTime'] = time.time()
    ss = ''
    for i in range(size):
        ss += seqM[seq]['collect'][i]
    del  seqM[seq]['collect']
    re = {'ret':1}
    re_set(key,ss)
    if not isTest():
        sock.sendto(json.dumps(re), addr)  
    continue    
    
    
#whatReady = select.select([sock], [], [], 0)
#if whatReady[0] == []: # Timeout
    #yield gen.sleep(0.01)      
#else:    
    #data, addr = sock.recvfrom(recLen)
    #m = json.loads(data)
      #sock.sendto(j, addr)    
