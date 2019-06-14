import socket,select,json,time
import base64

add = '127.0.0.1'
port = 9993
waitTime = 1
import re 
def cut_text(text,lenth): 
    textArr = re.findall('.{'+str(lenth)+'}', text) 
    textArr.append(text[(len(textArr)*lenth):]) 
packLimit = 700
sleepTime = 0.01
recLen = 10240

m = {'l':1100,'opt':'connect','seq':'101'}
j = json.dumps(m)    
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(j, (add, port))

x = sock.recv(10240)
print(x)

m = {'l':1100,'opt':'disconnect','seq':'101'}
j = json.dumps(m)    
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.sendto(j, (add, port))

x = sock.recv(10240)
print(x)

f = open('aa.txt','rb')
s = f.read()
f.close()
s = base64.b64encode(s)



m = {'pack':0,'data':s}   
j = json.dumps(m)    
sock.sendto(j, (add, 9992))

x = sock.recv(10240)
print(x)

m = {'pack':0,'hasGot':-1}  

j = json.dumps(m)    
sock.sendto(j, (add, 9991))
x = sock.recv(10240)
print(x)

sock.close()    

