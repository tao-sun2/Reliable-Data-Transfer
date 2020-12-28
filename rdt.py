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
SENT_SYN = 1
RECV_SYN = 2
CONNECTION = 3

# define some variables
MAX_RECEIVE_SIZE = 65536
TIME_OUT = 0.2


class RDTSocket(UnreliableSocket):
    def __init__(self, rate=None, debug=True):
        super().__init__(rate=rate)
        self.rate = rate
        self.debug = debug
        # the controller is to control send and receive for client
        self.controller = None
        # all_controllers is the queue of all controllers for server
        self.all_controllers = Queue()
        # the client_controller helps the server find the correct controller for a certain client address
        self.client_controller = {}
        # threaded_receiver is a new thread to handle message receive
        self.threaded_receiver = None

    def accept(self):
        """
        Accept a connection. The socket must be bound to an address and listening for
        connections. The return value is a pair (controller, address) where controller is a new
        RDTController object usable to send and receive data on the connection, and address
        is the address bound to the socket on the other end of the connection.
        This function should be blocking.
        1. receive SYN
        2. send SYNACK
        3. receive ACK
        """

        if not self.threaded_receiver:
            self.threaded_receiver = Thread(target=self.receive_threaded_server)
            self.threaded_receiver.start()
        controller = self.all_controllers.get(block=True)
        return controller, controller.to_address

    def connect(self, address: (str, int)):
        """
        Connect to a remote socket at address.
        Corresponds to the process of establishing a connection on the client side.
        1. send SYN
        2. receive SYNACK
        3. send SYN
        """

        controller = RDTController(address, self)
        self.controller = controller
        self.threaded_receiver = Thread(target=self.receive_threaded_client)
        self.threaded_receiver.start()
        controller.current_state = SENT_SYN
        controller.socket.sendto(RDTPacket.create(controller.seq, controller.ack, b'\xAC', SYN=True).to_bytes(),
                                 controller.to_address)
        controller.have_been_sent.append(
            (RDTPacket.create(controller.seq, controller.ack, b'\xAC', SYN=True), time.time()))

    # a threaded receiver for client
    def receive_threaded_client(self):
        while self.controller.threaded_receiver_on:
            try:
                data, addr = self.recvfrom(MAX_RECEIVE_SIZE)
                packet = RDTPacket.from_bytes(data)
                self.controller.all_received_packets.put(packet)
            except Exception:
                # byte corrupt happen
                continue

    # a threaded receiver for server
    def receive_threaded_server(self):
        while True:
            try:
                data, addr = self.recvfrom(MAX_RECEIVE_SIZE)
                if addr not in self.client_controller:
                    conn = RDTController(addr, self)
                    self.client_controller[addr] = conn
                    self.all_controllers.put(conn)
                packet = RDTPacket.from_bytes(data)
                self.client_controller[addr].all_received_packets.put(packet)
            except Exception:
                # byte corrupt happen
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

        return self.controller.recv(bufsize)

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
                self.controller.send(data[start:end])
                break
            self.controller.send(data[start:end])
            start = end
            end = start + size
            if end >= total:
                end = total

    def close(self):
        """
        Finish the connection and release resources. For simplicity, assume that
        after a socket is closed, neither futher sends nor receives are allowed.
        """
        if self.controller:
            # send a certain message to represent the end of the connection
            self.send(b'_3@)')
            self.send(b'_3@)')
            self.send(b'_3@)')
            self.send(b'_3@)')
            self.send(b'_3@)')
            time.sleep(1)
            self.controller.FSM_on = False
            self.controller.threaded_receiver_on = False
        super().close()


class RDTController:
    def __init__(self, address, socket):
        # the address of the host on the other side of the connection
        self.to_address = address
        # the socket used to send and receive message
        self.socket = socket
        self.seq = 0
        self.ack = 0
        self.current_state = CLOSED
        # all_received_packets is a queue of all received packets including ACK and SYN information
        self.all_received_packets = Queue()
        # received_data_packets a queue of all received data packets
        self.received_data_packets = Queue()
        # to_be_send is a queue to store all data to be send
        self.to_be_send = Queue()
        # have_been_send is a list to store all sended packets and their send time
        self.have_been_sent = []

        self.threaded_receiver_on = True
        self.FSM_on = True
        self.machine = Thread(target=self.FSM)
        self.machine.start()
        self.time_out = 0.3
        if not self.socket.rate:
            self.time_out = 1
        print(self.time_out)

    def recv(self, bufsize: int) -> bytes:
        data = self.received_data_packets.get(block=True).payload
        # a certain message to break the while which representing the end of the connection
        if data == b'_3@)':
            return b''
        else:
            return data

    def send(self, data: bytes):
        self.to_be_send.put(data)

    def close(self) -> None:
        self.FSM_on = False
        self.threaded_receiver_on = False

    def FSM(self):
        while self.FSM_on:
            # sent records all the packets that have been sent and their send time
            sent = self.have_been_sent
            self.have_been_sent = []
            for i in sent:
                packet = i[0]
                send_time = i[1]
                if self.seq >= packet.seq + packet.LEN:
                    continue
                if time.time() - send_time >= TIME_OUT:
                    self.socket.sendto(packet.to_bytes(), self.to_address)
                    self.have_been_sent.append((packet, time.time()))
                else:
                    self.have_been_sent.append((packet, send_time))

            if (self.all_received_packets.empty(), self.to_be_send.empty(), bool(self.have_been_sent),
                self.current_state == CONNECTION) == (
                    True, False, False, True):
                data = self.to_be_send.get()
                to_send = RDTPacket.create(self.seq, self.ack, data)
                self.socket.sendto(to_send.to_bytes(), self.to_address)
                self.have_been_sent.append((to_send, time.time()))

            try:
                packet = self.all_received_packets.get(timeout=self.time_out)
            except:
                continue

            if packet.LEN != 0 and self.ack > packet.seq:
                packet = RDTPacket.create(self.seq, self.ack, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.to_address)
                self.have_been_sent.append((packet, time.time()))
                continue
            if packet.LEN != 0 and self.ack < packet.seq:
                continue
            if packet.LEN != 0:
                if self.ack < packet.seq + packet.LEN:
                    self.ack = packet.seq + packet.LEN

            if packet.ACK:
                if self.seq < packet.ack:
                    self.seq = packet.ack

            if self.current_state == CLOSED and packet.SYN:
                self.current_state = RECV_SYN

                packet = RDTPacket.create(self.seq, self.ack, b'\xAC', SYN=True, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.to_address)
                self.have_been_sent.append((packet, time.time()))

            elif self.current_state == RECV_SYN and packet.ACK:
                assert packet.ack == 1
                self.current_state = CONNECTION
            elif self.current_state == SENT_SYN and packet.SYN:
                self.current_state = CONNECTION

                packet = RDTPacket.create(self.seq, self.ack, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.to_address)
                self.have_been_sent.append((packet, time.time()))

            elif packet.LEN != 0:
                self.received_data_packets.put(packet)
                packet = RDTPacket.create(self.seq, self.ack, ACK=True)
                self.socket.sendto(packet.to_bytes(), self.to_address)
                self.have_been_sent.append((packet, time.time()))


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
