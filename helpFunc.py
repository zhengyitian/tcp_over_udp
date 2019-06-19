import hashlib, binascii,time,uuid,json
import struct
miniSleep = 0.01
recLen = 10240
timeoutTime = 0.7
maxSending = 5
bufferSize = 1000
cacheSize = 100    
pushAhead = 100
def getMapLength(m):
    return len(json.dumps(m))
def torRet(r):
    e = StopIteration()
    e.value = r
    raise e  

class TOUMsg():
    def __init__(self,m = {},s=''):
        self.m_json = m
        self.strContetn = s
        self.length = 0
        
    def pack(self):
        j = json.dumps(self.m_json)
        jL = len(j)
        cL = len(self.strContetn)
        self.length = 16+jL+cL
        return struct.pack('q',jL)+j+struct.pack('q',cL)+self.strContetn
    
    def unpack(self,s):
        if len(s)<16:
            return False,s
        jL = struct.unpack('q',s[:8])[0]
        if len(s)<16+jL:
            return False,s
        cL = struct.unpack('q',s[8+jL:16+jL])[0]
        if len(s)<16+jL+cL:
            return False,s
        self.m_json = json.loads(s[8:8+jL])
        self.strContetn = s[16+jL:16+jL+cL]
        self.length = 16+jL+cL
        return True,s[16+jL+cL:]
    
import re as re3
def cut_text(text,lenth): 
    textArr = re3.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
    return textArr

def makePack(s,salt):
    u = str(uuid.uuid1())
    u = u.replace('-','')
    u2 = binascii.unhexlify(u)
    s1 = u2+s
    dk = hashlib.pbkdf2_hmac('md5', s1, salt, 2)
    s2 = s1+dk
    return u,s2
def makePack_server(s,u,salt):
    u2 = binascii.unhexlify(u)
    s1 = u2+s
    dk = hashlib.pbkdf2_hmac('md5', s1, salt, 2)
    s2 = s1+dk
    return s2
def checkPackValid(s,u,salt):
    if len(s)<16:
        return ''
    s1 = s[-16:]
    s2 = s[:-16]
    uuid = binascii.unhexlify(u)
    dk = hashlib.pbkdf2_hmac('md5', s2, salt, 2)
    if dk != s1:
        return ''
    if s2[:16] != uuid:
        return ''
    return s2[16:]

def checkPackValid_server(s,salt):
    if len(s)<16:
        return '',''
    s1 = s[-16:]
    s2 = s[:-16]
    dk = hashlib.pbkdf2_hmac('md5', s2, salt, 2)
    if dk != s1:
        return '',''
    if len(s2)<16:
        return '',''
    return binascii.hexlify(s2[:16]) ,s2[16:]


def circleBig(a,b,bufferSize=bufferSize):
    if a==b:
        return False
    if a>b and (a-b)<(bufferSize/2):
        return True
    if a<b and (b-a)>(bufferSize/2):
        return True
    return False

def circleRange(a,b,bufferSize=bufferSize):  # return [,)  same as range
    temp = a
    ret = []
    while True:
        if not circleBig(b,temp):
            break
        ret.append(temp)
        temp = circleAdd(temp,1)
    return ret
def circleMax(l,bufferSize=bufferSize):
    ret = None
    k = l.keys()
    for i in k:
        if ret==None:
            ret = i
        if circleBig(i,ret):
            ret = i
    return ret


def circleAddOne(a,bufferSize=bufferSize):
    if a == bufferSize-1:
        return 0
    return a+1

def circleAdd(a,b,bufferSize=bufferSize):
    ret = a
    for i in range(b):
        ret = circleAddOne(ret)
    return ret


if __name__ == '__main__':
    u,s = makePack('sdfew','salt')
    print u,s
    s2 = checkPackValid(s, u, 'salt')
    print s2

          