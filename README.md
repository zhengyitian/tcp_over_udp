# tcp_over_udp
version : python 2.7
run  redir_client2.py(needs redisMan.py in the same directory) on client
run redir_server2.py and redisS_singel.py on server
it directs tcp 127.0.0.1:9999 to youserver:8080 by using udp
now it works very slow, i believe there are some bugs in it.
i make these files because i found tcp to my server was blocked but udp not,
i also found big flow on one udp port would be slow over time,but changing a port would solve it,
so i make 100 udp ports.
the workings in it is:using udp to complement a redis service, then build tcp on the reliable redis service.
