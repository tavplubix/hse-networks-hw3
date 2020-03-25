
import socket
import signal
import logging
import time
import threading
import json

from src.server_backend import Server


class Session:
    def __init__(self, conn: socket.socket, addr):
        self.conn = conn
        self.conn.settimeout(600)
        self.client_addr = addr
        self.timeout = 600
        self.thread = None
        self.server = None

    def send_packet(self, obj):
        bdata = json.dumps(obj, default=str).encode()
        size = len(bdata)
        logging.debug(f'Sending object of size {size} to {self.client_addr}: {bdata}')
        packet = size.to_bytes(4, byteorder='big') + bdata
        self.conn.sendall(packet)
        logging.debug(f'Sent {size} bytes to {self.client_addr}')

    def recvall(self, size, time_left):
        bdata = bytes()
        while len(bdata) < size or 0 < time_left[0]:
            start = time.time() * 1000
            bdata += self.conn.recv(size - len(bdata))
            time_left[0] -= time.time() * 1000 - start

        if len(bdata) < size:
            raise TimeoutError('Timeout while receiving data')
        return bdata

    def recv_packet(self):
        time_left = [self.timeout]
        logging.debug(f'Waiting header from {self.client_addr}')
        bsize = self.recvall(4, time_left)
        size = int.from_bytes(bsize, byteorder='big')
        logging.debug(f'Receiving packet of size {size} from {self.client_addr}')
        bdata = self.recvall(size, time_left)
        logging.debug(f'Received packet of size {size} from {self.client_addr}: {bdata}')
        return json.loads(bdata.decode())

    def end(self):
        logging.info('Closing connection from ' + str(self.client_addr))
        self.conn.close()


class TCPServer:
    def __init__(self, host: str, port: int) -> None:
        logging.debug(f"Starting backend server")
        self.srv = Server()
        logging.debug(f"Backend server started")
        self.host = host
        self.port = port
        self.control_sock = socket.socket()
        self.control_sock.bind((self.host, self.port))
        self.control_sock.settimeout(5)
        self.control_sock.listen()
        print("Listen", self.host, self.port)
        self.shutdown = None
        self.sessions = set()
        self.sessions_lock = threading.Lock()

        def term_signal_handler(sig, arg):
            print("Got signal", sig)
            if self.shutdown:
                print("Stop now")
                exit(0)
            self.stop()

        signal.signal(signal.SIGTERM, term_signal_handler)
        signal.signal(signal.SIGINT, term_signal_handler)

    def run(self):
        print("Started")
        self.shutdown = False
        while not self.shutdown:
            try:
                conn, addr = self.control_sock.accept()
                print("Accepted connection from", addr)
                session = Session(conn, addr)
                session.server = self
                session.thread = threading.Thread(target=self.process_connection_thread, args=(session,))
                session.thread.daemon = True
                session.thread.start()
            except socket.timeout:
                pass
            except BaseException as e:
                print("ERROR: Got exception:", e)

    def process_connection_thread(self, session: Session):
        try:
            print(f"Begin processing connection from {session.client_addr}")
            with self.sessions_lock:
                self.sessions.add(session)
            self.process_connection(session)
        except BaseException as e:
            print(f"ERROR: Got exception while procession connection {session.client_addr}: {e}")
        finally:
            with self.sessions_lock:
                self.sessions.remove(session)
            print(f"End processing connection from {session.client_addr}")

    def process_connection(self, session: Session):
        while True:
            try:
                request = session.recv_packet()
                if request['method'] == 'end':
                    session.end()
                    break

                data = self.process_request(request)
                session.send_packet({'status': 'ok', 'data': data})

            except OSError as e:
                logging.info('Caught ' + str(e))
                session.end()
                break
            except Exception as e:
                logging.info('Caught ' + str(e))
                session.send_packet({'status': 'error', 'exception': str(e)})

    def process_request(self, request):
        method = request['method']
        time_start = request.get('time_start', None)
        time_end = request.get('time_end', None)

        if method == 'get_user_info':
            return self.srv.get_user_info(user_name=request['user_name'])
        if method == 'get_contingent_by_user_id':
            return self.srv.get_contingent_by_user_id(request['user_id'])
        if method == 'get_timetable':
            return self.srv.get_timetable(request['user_id'], time_start, time_end)
        if method == 'get_deadlines':
            return self.srv.get_deadlines(request['user_id'], time_start, time_end)

        if method == 'create_deadline':
            contingent_id = request.get("contingent_id")
            time = request.get("time")
            weight = float(request.get("weight", '0'))
            name = request.get("name")
            desc = request.get("desc", '')
            self.srv.create_deadilne(request['user_id'], contingent_id, time, weight, name, desc)
        elif method == 'change_deadline_estimate':
            self.srv.change_deadline_estimate(request['user_id'], request['deadline_id'], request['val'])
        elif method == 'change_deadline_real':
            self.srv.change_deadline_real(request['user_id'], request['deadline_id'], request['val'])
        else:
            raise Exception('Unknown method ' + str(method))

        return {}

    def stop(self):
        print("Shutting down ...")
        self.shutdown = True



