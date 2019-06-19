import struct
s = struct.pack('q',122334)
s2 = struct.unpack('q',s)[0]
print s2