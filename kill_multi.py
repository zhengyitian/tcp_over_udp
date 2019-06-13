import os,sys
l = os.listdir('pids')
for i in l:
    os.system('kill -9 %s'%i)