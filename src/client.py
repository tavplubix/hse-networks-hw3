import sys
import socket
import json
import time


class Connection:
    def __init__(self, host: str, port: int):
        self.host = sys.argv[1]
        self.port = int(sys.argv[2])
        self.conn = socket.socket()
        self.conn.settimeout(10)
        self.conn.connect((host, port))
        self.timeout = 10

    def send_packet(self, obj: dict) -> None:
        bdata = json.dumps(obj).encode()
        size = len(bdata)
        print(f'Sending object of size {size}: {bdata}')
        packet = size.to_bytes(4, byteorder='big') + bdata
        self.conn.sendall(packet)
        print(f'Sent {size} bytes')

    def recvall(self, size, time_left):
        bdata = bytes()
        while len(bdata) < size or 0 < time_left[0]:
            start = time.time() * 1000
            bdata += self.conn.recv(size - len(bdata))
            time_left[0] -= time.time() * 1000 - start
            print(f'Received {len(bdata)} of {size} bytes, {time_left} seconds left')

        if len(bdata) < size:
            raise TimeoutError('Timeout while receiving data')
        return bdata

    def recv_packet(self) -> dict:
        time_left = [self.timeout]
        print(f'Waiting header')
        bsize = self.recvall(4, time_left)
        size = int.from_bytes(bsize, byteorder='big')
        print(f'Receiving packet of size {size}')
        bdata = self.recvall(size, time_left)
        print(f'Received packet of size {size}: {bdata}')
        return json.loads(bdata.decode())

    def close(self) -> None:
        self.send_packet({'method': 'end'})
        self.conn.close()

c = Connection(sys.argv[1], int(sys.argv[2]))

for line in sys.stdin:
    c.send_packet(json.loads(line))
    print(c.recv_packet())

