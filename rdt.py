from queue import Queue
from threading import Thread
import time
from USocket import UnreliableSocket

'''
Remember to transmit in mode B to test the performance
send() is not blocked in rdt implementation
'''

# define some states
CLOSED = 0
LISTEN = 1
SYN_SENT = 2
SYN_RCVD = 3
ESTABLISHED = 4
FIN_WAIT_1 = 5
FIN_WAIT_2 = 6
TIME_WAIT = 7
CLOSE_WAIT = 8
LAST_ACK = 9

# define some variables
MAX_RECEIVE_SIZE = 65536
TIME_OUT = 0.2


class RDTSocket(UnreliableSocket):
    def __init__(self, rate=None, debug=True):
        super().__init__(rate=rate)
        self.rate = rate
        self.debug = debug
        self.state = CLOSED
        self.receiver = None
        self.unhandled_conns = Queue()
        self.connections = {}
        self.connection = None

    def accept(self):
        """
        Accept a connection. The socket must be bound to an address and listening for
        connections. The return value is a pair (conn, address) where conn is a new
        socket object usable to send and receive data on the connection, and address
        is the address bound to the socket on the other end of the connection.
        This function should be blocking.
        1. receive SYN
        2. send SYNACK
        3. receive ACK
        """
        self.state = LISTEN
        if not self.receiver:
            self.receiver = Thread(target=self.receive_threaded_server)
            self.receiver.start()
        conn = self.unhandled_conns.get()
        return conn, conn.client

    def connect(self, address: (str, int)):
        """
        Connect to a remote socket at address.
        Corresponds to the process of establishing a connection on the client side.
        1. send SYN
        2. receive SYNACK
        3. send SYN
        """

        conn = RDTController(address, self)
        self.connection = conn
        self.receiver = Thread(target=self.receive_threaded_client)
        self.receiver.start()
        conn.state = SYN_SENT
        conn.socket.sendto(RDTPacket.create(conn.seq, conn.ack, b'\xAC', SYN=True).to_bytes(), conn.client)
        conn.sending.append((RDTPacket.create(conn.seq, conn.ack, b'\xAC', SYN=True), time.time()))

    # a threaded receiver for client
    def receive_threaded_client(self):
        while self.connection.receive_data:
            try:
                data, addr = self.recvfrom(MAX_RECEIVE_SIZE)
                packet = RDTPacket.from_bytes(data)
                self.connection.receive.put(packet)
            except Exception:
                continue

    # a threaded receiver for server
    def receive_threaded_server(self):
        while True:
            try:
                data, addr = self.recvfrom(MAX_RECEIVE_SIZE)
                if addr not in self.connections:
                    conn = RDTController(addr, self)
                    self.connections[addr] = conn
                    self.unhandled_conns.put(conn)
                packet = RDTPacket.from_bytes(data)
                self.connections[addr].receive.put(packet)
            except Exception:
                continue

    def recv(self, bufsize: int) -> bytes:
        """
        Receive data from the socket.
        The return value is a bytes object representing the data received.
        The maximum amount of data to be received at once is specified by bufsize.
        Note that ONLY data send by the peer should be accepted.
        In other words, if someone else sends data to you from another address,
        it MUST NOT affect the data returned by this function.
        """

        return self.connection.recv(bufsize)

    def send(self, data: bytes):
        """
        Send data to the socket.
        The socket must be connected to a remote socket
        i.e. self._send_to must not be none.
        """

        # set payload size, here I set payload size to 1900 bytes in every packet
        total = len(data)
        start = 0
        end = 0
        size = 1900
        end = start + size
        if end >= total:
            end = total
        while end <= total:
            if end == total:
                self.connection.send(data[start:end])
                break
            self.connection.send(data[start:end])
            start = end
            end = start + size
            if end >= total:
                end = total

    def close(self):
        """
        Finish the connection and release resources. For simplicity, assume that
        after a socket is closed, neither futher sends nor receives are allowed.
        """
        if self.connection:
            # send a certain message to represent the end of the connection
            self.send(b'_3@)')
            time.sleep(0.2)
            self.send(b'_3@)')
            time.sleep(0.2)
            self.send(b'_3@)')
            time.sleep(0.2)
            self.connection.on = False
            self.connection.receive_data = False
        super().close()


class RDTController:
    def __init__(self, client, socket):
        self.client = client
        self.socket = socket
        self.receive_data = True
        self.state = CLOSED
        self.seq = 0
        self.ack = 0
        self.receive = Queue()
        self.sends = Queue()
        self.message = Queue()
        self.sending = []
        self.on =True
        self.machine = Thread(target=self.FSM)
        self.machine.start()

    def recv(self, bufsize: int) -> bytes:
        data = self.message.get(block=True).payload
        # a certain message to break the while which representing the end of the connection
        if data == b'_3@)':
            return b''
        else:
            return data

    def send(self, data: bytes):
        self.sends.put(data)

    def close(self) -> None:
        self.on = False
        self.receive_data = False

    def FSM(self):
        while self.on:
            now = time.time()
            sending = self.sending

            self.sending = []  # 已经发过的包
            for packet, send_time in sending:
                if self.seq >= packet.seq + packet.LEN:
                    continue
                if now - send_time >= TIME_OUT:

                    self.socket.sendto(packet.to_bytes(), self.client)
                    self.sending.append((packet, time.time()))
                else:
                    self.sending.append((packet, send_time))

            # send data
            if self.receive.empty() and (not self.sends.empty()) and \
                    len(self.sending) == 0 and self.state == ESTABLISHED:
                data = self.sends.get()
                to_send = RDTPacket.create(self.seq, self.ack, data)
                self.socket.sendto(to_send.to_bytes(), self.client)
                self.sending.append((to_send, time.time()))

            # receive date

            try:
                if not self.socket.rate:

                    packet = self.receive.get(timeout=1)
                else:
                    packet = self.receive.get(timeout=0.3)


            except:

                continue

            if packet.LEN != 0 and packet.seq < self.ack:
                packet = RDTPacket.create(self.seq, self.ack, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.client)
                self.sending.append((packet, time.time()))
                continue
            if packet.LEN != 0 and packet.seq > self.ack:
                continue
            if packet.ACK:
                self.seq = max(self.seq, packet.ack)
            if packet.LEN != 0:
                self.ack = max(self.ack, packet.seq + packet.LEN)
            if self.state == CLOSED and packet.SYN:
                self.state = SYN_RCVD

                packet = RDTPacket.create(self.seq, self.ack, b'\xAC', SYN=True, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.client)
                self.sending.append((packet, time.time()))
            elif self.state == SYN_SENT and packet.SYN:
                self.state = ESTABLISHED

                packet = RDTPacket.create(self.seq, self.ack, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.client)
                self.sending.append((packet, time.time()))

            elif self.state == SYN_RCVD and packet.ACK:
                assert packet.ack == 1
                self.state = ESTABLISHED



            elif packet.LEN != 0:
                self.message.put(packet)

                packet = RDTPacket.create(self.seq, self.ack, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.client)
                self.sending.append((packet, time.time()))





class RDTPacket:
    def __init__(self):
        self.SYN = False
        self.ACK = False
        self.FIN = False
        self.seq = 0
        self.ack = 0
        self.LEN = 0
        self.CHECKSUM = 0
        self.payload = b''

    def to_bytes(self):
        data = b''
        flag = 0
        if self.SYN:
            flag += 0x8000
        if self.ACK:
            flag += 0x4000
        if self.FIN:
            flag += 0x2000
        data += int.to_bytes(flag, 2, byteorder='big')
        data += int.to_bytes(self.seq, 4, byteorder='big')
        data += int.to_bytes(self.ack, 4, byteorder='big')
        data += int.to_bytes(self.LEN, 4, byteorder='big')
        data += int.to_bytes(self.CHECKSUM, 2, byteorder='big')
        data += self.payload

        if self.LEN % 2 == 1:
            data += b'\x00'

        return data

    @staticmethod
    def from_bytes(byte: bytes):
        packet = RDTPacket()
        flag = int.from_bytes(byte[0:2], byteorder='big')
        if flag & 0x8000 != 0:
            packet.SYN = True
        if flag & 0x4000 != 0:
            packet.ACK = True
        if flag & 0x2000 != 0:
            packet.FIN = True
        packet.seq = int.from_bytes(byte[2:6], byteorder='big')
        packet.ack = int.from_bytes(byte[6:10], byteorder='big')
        packet.LEN = int.from_bytes(byte[10:14], byteorder='big')
        packet.CHECKSUM = int.from_bytes(byte[14:16], byteorder='big')
        packet.payload = byte[16:]

        if packet.LEN % 2 == 1:
            packet.payload = packet.payload[:-1]

        assert packet.LEN == len(packet.payload)
        assert RDTPacket.checksum(packet.to_bytes()) == 0

        return packet

    @staticmethod
    def create(seq=0, ack=0, data=b'', SYN=False, ACK=False, FIN=False):
        packet = RDTPacket()
        packet.ACK = ACK
        packet.FIN = FIN
        packet.SYN = SYN

        packet.seq = seq
        packet.ack = ack
        packet.LEN = len(data)

        packet.payload = data
        packet.CHECKSUM = 0
        checksum = RDTPacket.checksum(packet.to_bytes())
        packet.CHECKSUM = checksum

        return packet

    @staticmethod
    def checksum(data: bytes):
        length = len(data)
        sum = 0
        for i in range(0, int(length / 2)):
            b = int.from_bytes(data[0: 2], byteorder='big')

            data = data[2:]
            sum = (sum + b) % 65536
        return (65536 - sum) % 65536
