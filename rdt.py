from datetime import datetime
from queue import Queue
from threading import Thread, currentThread
from enum import Enum, auto
from typing import Tuple, List, Dict
import time
from USocket import UnreliableSocket

'''
Remember to transmit in mode B to test the performance
send() is not blocked in rdt implementation
'''
Address = Tuple[str, int]

class Packet:
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
        packet = Packet()
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
        assert Packet.checksum(packet.to_bytes()) == 0

        return packet

    @staticmethod
    def create(seq=0, ack=0, data=b'', SYN=False, ACK=False, FIN=False):
        packet = Packet()
        packet.ACK = ACK
        packet.FIN = FIN
        packet.SYN = SYN

        packet.seq = seq
        packet.ack = ack
        packet.LEN = len(data)

        packet.payload = data
        packet.CHECKSUM = 0
        checksum = Packet.checksum(packet.to_bytes())
        packet.CHECKSUM = checksum

        return packet

    @staticmethod
    def checksum(data: bytes):
        length = len(data)
        sum = 0
        for i in range(0, int(length / 2)):
            b = int.from_bytes(data[0: 2], byteorder='big')
            # print(hex(data[0]), hex(data[1]))
            data = data[2:]
            sum = (sum + b) % 65536
        return (65536 - sum) % 65536

    def __str__(self) -> str:
        res = ""

        if self.SYN:
            res += "\033[94mSYN\033[0m "
        if self.ACK:
            res += "\033[93mACK\033[0m "
        if self.FIN:
            res += "\033[91mFIN\033[0m "

        res += "["
        res += "seq={}, ".format(self.seq)
        res += "ack={}, ".format(self.ack)

        if self.LEN != 0:
            res += "Len={}, ".format(self.LEN)
            res += "] "
            res += str(self.payload)
        else:
            res += "] "

        return res

class State(Enum):
    CLOSED = auto()
    LISTEN = auto()
    SYN_SENT = auto()
    SYN_RCVD = auto()
    ESTABLISHED = auto()
    FIN_WAIT_1 = auto()
    FIN_WAIT_2 = auto()
    TIME_WAIT = auto()
    CLOSE_WAIT = auto()
    LAST_ACK = auto()


class StateMachine(Thread):
    def __init__(self, conn):
        Thread.__init__(self)
        self.conn: Connection = conn
        self.alive = True

    def run(self):
        conn = self.conn
        socket = conn.socket

        no_packet = 0
        cnt = 0
        while self.alive:
            now = datetime.now().timestamp()

            sending = conn.sending
            conn.sending = []
            for packet, send_time in sending:
                if conn.seq >= packet.seq + packet.LEN:
                    continue
                if now - send_time >= 1.0:
                    print(conn.state, "retransmit ", end='')
                    conn.send_packet(packet)
                else:
                    conn.sending.append((packet, send_time))

            # close
            if conn.state == State.TIME_WAIT and no_packet >= 6:
                conn.state = State.CLOSED
                print(conn.state)
                conn.close_connection()

            # send data
            if len(conn.receive.queue) == 0 and len(conn.sends.queue) != 0 and \
                    len(conn.sending) == 0 and no_packet >= 3 and conn.state in (State.ESTABLISHED, State.FIN_WAIT_1):
                data = conn.sends.get()
                if isinstance(data, Packet):
                    to_send = Packet.create(conn.seq, conn.ack, data.payload, SYN=data.SYN, ACK=data.ACK, FIN=data.FIN)
                else:
                    to_send = Packet.create(conn.seq, conn.ack, data)
                print(conn.state, "send ", end='')
                conn.send_packet(to_send)

            # receive date
            packet: Packet
            try:
                packet = conn.receive.get(block=False)
                no_packet = 0
            except:
                no_packet += 1
                continue

            print(conn.state, "recv", packet)

            if packet.LEN != 0 and packet.seq < conn.ack:
                print(conn.state, "resend ", end='')
                conn.send_packet(Packet.create(conn.seq, conn.ack, ACK=True))
                continue
            if packet.LEN != 0 and packet.seq > conn.ack:
                print(conn.state, "unordered ", packet)
                continue
            if packet.ACK:
                conn.seq = max(conn.seq, packet.ack)
            if packet.LEN != 0:
                conn.ack = max(conn.ack, packet.seq + packet.LEN)

            not_arrive = [it for (it, send_time) in conn.sending if conn.seq < it.seq + it.LEN]
            all_packet_arrive = len(conn.sends.queue) == 0 and len(not_arrive) == 0

            if conn.state == State.CLOSED and packet.SYN:
                conn.state = State.SYN_RCVD
                print(conn.state, "send ", end='')
                conn.send_packet(Packet.create(conn.seq, conn.ack, b'\xAC', SYN=True, ACK=True))
            elif conn.state == State.SYN_SENT and packet.SYN:
                conn.state = State.ESTABLISHED
                print(conn.state, "send ", end='')
                conn.send_packet(Packet.create(conn.seq, conn.ack, ACK=True))
            elif conn.state == State.SYN_RCVD and packet.ACK:
                assert packet.ack == 1
                conn.state = State.ESTABLISHED
            # close
            elif conn.state == State.ESTABLISHED and packet.FIN:
                conn.send_packet(Packet.create(conn.seq, conn.ack, ACK=True))
                conn.state = State.CLOSE_WAIT
                if all_packet_arrive:
                    conn.send_packet(Packet.create(conn.seq, conn.ack, b'\xAF', FIN=True, ACK=True))
                    conn.state = State.LAST_ACK
            elif conn.state == State.FIN_WAIT_1 and all_packet_arrive:
                conn.state = State.FIN_WAIT_2
                if packet.FIN and packet.ACK:
                    conn.send_packet(Packet.create(conn.seq, conn.ack, ACK=True))
                    conn.state = State.TIME_WAIT
            elif conn.state == State.CLOSE_WAIT and all_packet_arrive:
                conn.send_packet(Packet.create(conn.seq, conn.ack, b'\xAF', FIN=True, ACK=True))
                conn.state = State.LAST_ACK
            elif conn.state in (State.FIN_WAIT_1, State.FIN_WAIT_2) and packet.FIN and packet.ACK:
                conn.send_packet(Packet.create(conn.seq, conn.ack, ACK=True))
                conn.state = State.TIME_WAIT
            elif conn.state == State.LAST_ACK and packet.ACK:
                conn.state = State.CLOSED
                print(conn.state)
                conn.close_connection()

            elif packet.LEN != 0:
                print(time.time())
                conn.message.put(packet)
                print(conn.state, "send ", end='')
                conn.send_packet(Packet.create(conn.seq, conn.ack, ACK=True))


class Connection:
    def __init__(self, client: Address, socket):
        self.client = client
        self.socket = socket
        self.receive_data = True
        self.state = State.CLOSED
        self.seq = 0
        self.ack = 0
        self.receive: Queue[Packet] = Queue()
        self.sends: Queue[bytes] = Queue()
        self.message: Queue[Packet] = Queue()
        self.sending: List[Tuple[Packet, float]] = []

        self.machine = StateMachine(self)
        self.machine.start()

    def recv(self, bufsize: int) -> bytes:
        return self.message.get(block=True).payload

    def send(self, data: bytes) -> int:
        assert self.state not in (State.CLOSED, State.LISTEN,
                                  State.FIN_WAIT_1, State.FIN_WAIT_2, State.CLOSE_WAIT,
                                  State.TIME_WAIT, State.LAST_ACK)
        print("push", len(data), "bytes")
        self.sends.put(data)
        return len(data)

    def close(self) -> None:
        assert self.state in (State.SYN_RCVD, State.ESTABLISHED)
        self.sends.put(Packet.create(data=b'\xAF', FIN=True))
        self.state = State.FIN_WAIT_1

    def send_packet(self, packet: Packet):
        print(packet)
        self.socket.sendto(packet.to_bytes(), self.client)
        self.sending.append((packet, datetime.now().timestamp()))

    def on_recv_packet(self, packet: Packet):
        self.receive.put(packet)

    def close_connection(self):
        self.machine.alive = False
        self.receive_data = False
        self.socket._close_connection(self)



class RDTSocket(UnreliableSocket):
    def __init__(self, rate=None, debug=True):
        super().__init__(rate=rate)
        self._rate = rate
        self.debug = debug
        self.state = State.CLOSED
        self.receiver = None
        self.unhandled_conns: Queue = Queue()
        self.connections: Dict[Address, Connection] = {}
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
        assert self.state in (State.CLOSED, State.LISTEN)
        self.state = State.LISTEN

        def receive():
            while True:
                try:
                    data, addr = self.recvfrom(10 * 1024 * 1024)
                    if addr not in self.connections:
                        conn = Connection(addr, self)
                        self.connections[addr] = conn
                        self.unhandled_conns.put(conn)
                    packet = Packet.from_bytes(data)
                    self.connections[addr].on_recv_packet(packet)
                except:
                    pass

        if not self.receiver:
            self.receiver = Thread(target=receive)
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
        assert self.state == State.CLOSED

        conn = Connection(address, self)
        self.connection = conn

        def receive():
            while conn.receive_data:
                try:
                    data, addr = self.recvfrom(10 * 1024 * 1024)
                    packet = Packet.from_bytes(data)
                    conn.on_recv_packet(packet)
                except:
                    pass

        self.receiver = Thread(target=receive)
        self.receiver.start()

        conn.state = State.SYN_SENT
        conn.send_packet(Packet.create(conn.seq, conn.ack, b'\xAC', SYN=True))


    def recv(self, bufsize: int) -> bytes:
        """
        Receive data from the socket.
        The return value is a bytes object representing the data received.
        The maximum amount of data to be received at once is specified by bufsize.

        Note that ONLY data send by the peer should be accepted.
        In other words, if someone else sends data to you from another address,
        it MUST NOT affect the data returned by this function.
        """

        assert self.connection
        return self.connection.recv(bufsize)

    def send(self, data: bytes) -> int:
        """
        Send data to the socket.
        The socket must be connected to a remote socket
        i.e. self._send_to must not be none.
        """
        assert self.connection
        total = len(data)
        start = 0
        end = 0
        size = 4096
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
        return 1

    def close(self) -> None:
        """
        Finish the connection and release resources. For simplicity, assume that
        after a socket is closed, neither futher sends nor receives are allowed.
        """
        if self.connection:  # client
            self.connection.close()
        elif self.connections:  # server
            for conn in self.connections.values():
                conn.close()
            self.state = State.CLOSED
        else:
            raise Exception("Illegal state")

    def _close_connection(self, conn) -> None:
        if self.connection:  # client
            UnreliableSocket.close(self)
        elif self.connections:  # server
            del self.connections[conn.client]
            if self.state == State.CLOSED and len(self.connections) == 0:
                UnreliableSocket.close(self)
        else:
            raise Exception("Illegal state")





