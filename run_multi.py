import os,sys
os.system('rm pids -rf')
for i in range(10000,10100):
    print i
    os.system('nohup python redisS.py %s &'%i)