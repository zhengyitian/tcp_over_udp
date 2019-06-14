from tornado.ioloop import IOLoop
from tornado.options import define, options
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.tcpclient import TCPClient
from tornado.tcpserver import TCPServer
import functools,time
from concurrent.futures import ThreadPoolExecutor
import socket,json,select
import base64
import re 
def cut_text(text,lenth): 
    textArr = re.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
packLimit = 700
sleepTime = 0.01
recLen = 10240
connRecord = {}
availPort = {}
for i in range(11000,15000):
    availPort[i]=0
import random

def findPort():
    k = availPort.keys()
    b = random.sample(k, 1)[0]   
    del availPort[b]
    return b

@gen.coroutine
def cliEcho(streamS,streamC):
    while True:    
        d1 = yield streamC.read_bytes(1)            
        if len(d1)!=0:
            break
    
    data =  streamC._read_buffer
    l = len(data)
    d2 = yield streamC.read_bytes(l-1)
    try:
        yield streamS.write(d1+d2)
    except:
        streamC.close()
        raise Exception
    
@gen.coroutine
def run_client(streamS,streamC):
    while True:
        try:
            yield cliEcho(streamS,streamC)
        except :
            print("server " + " left.")
            streamS.close = True
            break



sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('127.0.0.1',9993))
print('Bind UDP on 9999...')

class udpS1():
    def __init__(self,po):
        self._read_buffer = ''
        sock = self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', po))
        IOLoop.instance().add_callback(functools.partial(self.backG))
        self.packM = {}
        self.nextPack = 0
        self.hasGot = {}
        self.closeSelf = False
        self.close = False
        self.manager = None
        self.po = po
    @gen.coroutine    
    def backG(self):
        while True:
            if self.closeSelf:
                self.sock.close()
                global availPort
                availPort[self.po] = 0
                break            
            whatReady = select.select([self.sock], [], [], 0)
            if whatReady[0] == []: # Timeout
                yield gen.sleep(0.01)

            else:    
                data, addr = self.sock.recvfrom(recLen)
                if self.close and not self.packM:
                    ret = {'close':1}
                    j = json.dumps(ret)
                    self.sock.sendto(j, addr)   
                    continue
                
                m = json.loads(data)
                pack = m['pack']
                hasGot = m['hasGot']
                k = self.packM.keys()
                for i in k:
                    if i<= hasGot:
                        del self.packM[i]
                k2 = self.hasGot.keys()
                for i in k2:
                    if i<= hasGot:
                        del self.hasGot[i]                        
                re = {}
                if len(k)==0:
                    ma = -1
                else:
                    ma = max(k)
                if pack not in self.packM:
                    re['ret'] = 0
                else:
                    re['ret'] = 1
                    re['data'] = base64.b64encode(self.packM[pack])
                    if pack not in self.hasGot:
                        self.hasGot[pack] = 0
                re['max'] = ma
                j = json.dumps(re)                
                self.sock.sendto(j, addr)                   
        
    @gen.coroutine        
    def connect(self):
        pass
    

    @gen.coroutine        
    def write(self,s):
        if self.closeSelf:
            raise Exception             
        l = cut_text(s,packLimit)
        for one in l:
            while len(self.packM.keys())>10000:
                if self.closeSelf:
                    raise Exception                        
                yield gen.sleep(0.01)
            self.write2(one)
      
    def write2(self,s):  
        if len(s)==0:
            return        
        k = self.packM.keys()        
        if len(k)==0:
            self.packM[0]=''
            ma = 0
        else:
            ma = max(k)
        if ma not in self.hasGot:
            ori = self.packM[ma]
            if len(ori)+ len(s)<= packLimit:
                self.packM[ma] = ori+s
            else:
                self.packM[ma] = ori+s[:packLimit-len(ori)]
                self.packM[ma+1] = s[packLimit-len(ori):]
        else:
            self.packM[ma+1] = s
        pass

class udpS2():
    def __init__(self,po):
        self._read_buffer = ''
        sock = self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', po))
        IOLoop.instance().add_callback(functools.partial(self.backG))
        self.packM = {}
        self.close = False
        self.closeSelf = False
        self.nextPack = 0
        self.manager = None
        self.po = po
        
    @gen.coroutine    
    def backG(self):
        while True:
            if self.closeSelf:
                self.sock.close()
                global availPort
                availPort[self.po] = 0                
                break
            whatReady = select.select([self.sock], [], [], 0)
            if whatReady[0] == []: # Timeout
                yield gen.sleep(0.01)          
            else:    
                data, addr = self.sock.recvfrom(recLen)
                if self.close and not self.packM :
                    ret = {'close':1}
                    j = json.dumps(ret)
                    self.sock.sendto(j, addr)   
                    continue
                
                m = json.loads(data)
                pack = m['pack']
                data = m['data']
                print data
                data = base64.b64decode(data)
                self.packM[pack] = data
                while self.nextPack in self.packM:
                    self._read_buffer += self.packM[self.nextPack]
                    self.nextPack += 1
                  
                ret = {'pack':self.nextPack}
                j = json.dumps(ret)
                self.sock.sendto(j, addr)                   
    
   
    @gen.coroutine        
    def read_bytes(self):
        if self.closeSelf:
            raise Exception        
        s = self._read_buffer
        while len(s)==0:
            if self.closeSelf:
                raise Exception                 
            yield gen.sleep(0.01)
        self._read_buffer = ''
        e = StopIteration()
        e.value = s
        raise e                


@gen.coroutine
def echo(stream,streamCli):
    while True:    
        try:
            d1 =  yield  stream.read_bytes()
        except:
            streamCli.close()
            raise Exception
        if len(d1)!=0:
            break   
    yield streamCli.write(d1)
    


        
@gen.coroutine 
def deal_disconn(sock,addr,m2):
    global connRecord
    seq = m2['seq']
    if seq not in connRecord:
        m = {'ret':0}
        j = json.dumps(m)
        sock.sendto(j, addr)    
        return   
    u1 ,u2 = connRecord[seq] 
    del connRecord[seq] 
    u1.closeSelf = True
    u2.closeSelf = True
    
    m = {'ret':1}
    j = json.dumps(m)
    sock.sendto(j, addr)           
@gen.coroutine 
def deal_conn(sock,addr,m2):
    global connRecord
    seq = m2['seq']
    if seq in connRecord:
        if connRecord[seq]==-1:        
            m = {'ret':-1}
        else:
            m = {'ret':0,'port1':connRecord[seq][0].po,'port2':connRecord[seq][1].po}            
        j = json.dumps(m)
        sock.sendto(j, addr)    
        return       
    ip = '0.0.0.0'
    try:
        streamCli = yield TCPClient().connect(ip, 8080)       
    except:
        m = {'ret':-1}
        connRecord[seq] = -1
        j = json.dumps(m)
        sock.sendto(j, addr)    
        return       
    
    m = {'ret':1}
    p1 = findPort()
    p2 = findPort()
    m['port1'] = p1
    m['port2'] = p2     

  
    u1 = udpS1(p1)
    u2 = udpS2(p2)   
    connRecord[seq] = [u1,u2]
    
    IOLoop.instance().add_callback(functools.partial(run_client,u1,streamCli))
    j = json.dumps(m)
    sock.sendto(j, addr)       
    while True:
        try:
            yield echo(u2,streamCli)
        except :
            print("Client "  + " left.") 
            u2.close = True            
            break    

@gen.coroutine
def backG():
    while True:
        whatReady = select.select([sock], [], [], 0)
        if whatReady[0] == []: # Timeout
            yield gen.sleep(0.01)      
        else:    
            data, addr = sock.recvfrom(recLen)
            m = json.loads(data)
            opt = m['opt']
            if opt == 'connect':
                IOLoop.instance().add_callback(functools.partial(deal_conn,sock,addr,m))
 
            elif opt == 'disconnect':
                IOLoop.instance().add_callback(functools.partial(deal_disconn,sock,addr,m))
 

if __name__ == "__main__":
    IOLoop.instance().add_callback(backG)
    IOLoop.instance().start()
    




