import sys
import socket
import json
import time
import csv


class Connection:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.conn = socket.socket()
        self.conn.settimeout(5)
        self.conn.connect((host, port))
        self.timeout = 5

    def send_packet(self, obj: dict) -> None:
        bdata = json.dumps(obj).encode()
        size = len(bdata)
        #print(f'Sending object of size {size}: {bdata}')
        packet = size.to_bytes(4, byteorder='big') + bdata
        self.conn.sendall(packet)
        #print(f'Sent {size} bytes')

    def recvall(self, size, time_left):
        bdata = bytes()
        while len(bdata) < size and 0 < time_left[0]:
            start = time.time()
            bdata += self.conn.recv(size - len(bdata))
            time_left[0] -= time.time() - start
            #print(f'Received {len(bdata)} of {size} bytes, {time_left} seconds left')

        if len(bdata) < size:
            raise TimeoutError('Timeout while receiving data')
        return bdata

    def recv_packet(self) -> dict:
        time_left = [self.timeout]
        #print(f'Waiting header')
        bsize = self.recvall(4, time_left)
        size = int.from_bytes(bsize, byteorder='big')
        #print(f'Receiving packet of size {size}')
        bdata = self.recvall(size, time_left)
        #print(f'Received packet of size {size}: {bdata}')
        return json.loads(bdata.decode())

    def close(self) -> None:
        try:
            self.send_packet({'method': 'end'})
        except:
            pass
        self.conn.close()


class Client:
    def __init__(self):
        self.c = None

    def run(self):
        print('cmd> ', end='')
        sys.stdout.flush()
        while True:
            try:
                line = sys.stdin.readline()
                tokens = [x.strip() for x in line.split(' ') if len(x.strip()) != 0]
                self.process_command(tokens)
            except TimeoutError:
                print('Timeout. Disconnected.')
                self.c = None
            except ConnectionResetError:
                print('Connection reset by peer ')
                self.c = None
            except Exception as e:
                print('ERROR:', type(e), e)
            except KeyboardInterrupt:
                print('Type "q" to exit')
            print('cmd> ', end='')
            sys.stdout.flush()

    def request(self, data: dict):
        if self.c is None:
            raise Exception('Not connected. Use command "connect <host> <port>"')
        self.c.send_packet(data)
        res = self.c.recv_packet()
        if res['status'] != 'ok':
            raise Exception(res['exception'])
        return res['data']

    def drop_unneeded(self, row: dict, unneeded=['flow', 'course_name_short', 'deadlines_description']):
        for col in unneeded:
            row.pop(col, None)
        return row

    def print_array(self, data: list):
        if len(data) == 0:
            print('No results')
            return
        writer = csv.DictWriter(sys.stdout, fieldnames=[key for key in self.drop_unneeded(data[0])], delimiter='\t')
        writer.writeheader()
        for row in data:
            writer.writerow(self.drop_unneeded(row))
        print(f'Total: {len(data)} rows')

    def process_command(self, tokens):
        if len(tokens) == 0:
            return
        elif tokens[0] == 'q':
            quit()
        elif tokens[0] == 'help':
            print('TODO')

        elif tokens[0] == 'connect':
            self.c = Connection(tokens[1], int(tokens[2]))
            print('ok')
        elif tokens[0] == 'disconnect':
            if self.c is not None:
                self.c.close()
                self.c = None
            print('ok')

        elif tokens[0] == 'students':
            self.print_array(self.request({'method': 'get_user_info', 'user_name': tokens[1]}))
        elif tokens[0] == 'groups':
            req = {'method': 'get_contingent_by_user_id'}
            if 1 < len(tokens):
                req['user_id'] = tokens[1]
            self.print_array(self.request(req))
        elif tokens[0] == 'lessons':
            req = {'method': 'get_timetable'}
            if 1 < len(tokens):
                req['user_id'] = tokens[1]
            self.print_array(self.request(req))
        elif tokens[0] == 'deadlines':
            self.print_array(self.request({'method': 'get_deadlines'}))

        elif tokens[0] == 'new' and tokens[1] == 'deadline':
            req = {'method': 'create_deadline'}
            req['contingent_id'] = int(tokens[2])
            req['time'] = tokens[3]
            req['name'] = tokens[4]
            self.request(req)
            print('ok')
        elif tokens[0] == 'deadline' and tokens[1] == 'estimated':
            req = {'method': 'change_deadline_estimate'}
            req['deadline_id'] = int(tokens[3])
            req['val'] = float(tokens[4])
            self.request(req)
            print('ok')
        elif tokens[0] == 'deadline' and tokens[1] == 'real':
            req = {'method': 'change_deadline_real'}
            req['deadline_id'] = int(tokens[3])
            req['val'] = float(tokens[4])
            self.request(req)
            print('ok')

        elif tokens[0] == 'register':
            self.request({'method': 'register', 'login': tokens[1], 'password': tokens[2], 'student_id': tokens[3]})
            print('ok')
        elif tokens[0] == 'login':
            self.request({'method': 'login', 'login': tokens[1], 'password': tokens[2]})
            print('ok')
        elif tokens[0] == 'logout':
            self.request({'method': 'logout'})
            print('ok')
        else:
            raise Exception(f'Unknown command "{" ".join(tokens)}". Try command "help"')




#c = Connection(sys.argv[1], int(sys.argv[2]))

#for line in sys.stdin:
#    c.send_packet(json.loads(line))
#    print(c.recv_packet())

c = Client()
c.run()

