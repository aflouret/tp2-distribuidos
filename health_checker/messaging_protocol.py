from socket import socket
import logging

# Config
ENDIANNES = "big"
OP_CODE_BYTES = 1
DATA_LENGHT_BYTES = 4
HEADER_LENGHT = OP_CODE_BYTES + DATA_LENGHT_BYTES
MAX_PACKET_SIZE = 8192

# Encoders and decoders

def encode(data):

    if type(data) == int:
        return str(data).encode()

    if type(data) == str:
        return data.encode()

    if type(data) == list:
        return ("#".join(data) + "#").encode()

    if type(data) == bytes:
        return data


def decode(data):
    decoded = data.decode()
    if decoded.isdigit():
        return int(decoded)

    if decoded.find("#") > 0:
        return decoded.split("#")[:-1]

    return decoded


# Application layer data packet, based on TLV format: Type - Lenght - Value

class Packet:

    def __init__(self, opcode: int = None, data_lenght: int = None, data: bytes = None):
        self.opcode = opcode
        self.data_lenght = data_lenght
        self.data = data

    @classmethod
    def new(cls, opcode, data):
        encoded_data = encode(data)
        return Packet(opcode, len(encoded_data), encoded_data)

    def get(self):
        return decode(self.data)


class ErrorPacket(Packet):

    def __init__(self, msg: str):
        super().__init__()
        self.opcode = -1
        self.data = msg.encode()


# Upper Layer

class ShortReadException(Exception):
    pass


class ShortWriteException(Exception):
    pass


class ReadZeroException(Exception):
    pass


def receive(s: socket):
    # Receive header
    try:
        read_bytes = __receive(s, HEADER_LENGHT)
    except ShortReadException:
        return ErrorPacket("Short read. Cadena de bytes invalida")
    except ReadZeroException:
        return Packet.new(0,"")

    opcode = int.from_bytes(read_bytes[:OP_CODE_BYTES], byteorder=ENDIANNES)
    data_lenght = int.from_bytes(read_bytes[OP_CODE_BYTES:], byteorder=ENDIANNES)

    # Receive data
    data = b""

    while len(data) < data_lenght:

        to_read = min(data_lenght - len(data), MAX_PACKET_SIZE)
        try:
            read_bytes = __receive(s, to_read)

        except (ShortReadException, ReadZeroException):
            logging.error(f'action: receive | se interpretó un ShortReadException o ReadZeroException, posible deadlock')
            return ErrorPacket("Short read. Cadena de bytes invalida")

        data += read_bytes

    return Packet(opcode, data_lenght, data)


def send(s: socket, packet: Packet):
    # send header
    encoded_header = packet.opcode.to_bytes(OP_CODE_BYTES, ENDIANNES)\
                     + packet.data_lenght.to_bytes(DATA_LENGHT_BYTES, ENDIANNES)
    __send(s, encoded_header)

    # send data
    i, offset = 0, 0
    total_sent = 0

    while total_sent < packet.data_lenght:

        i, offset = i + offset, min(packet.data_lenght - total_sent, MAX_PACKET_SIZE)
        try:
            sent_bytes = __send(s, packet.data[i: i + offset])
        except ShortWriteException as e:
            logging.error(f'action: send | se interpretó un ShortWrite, posible deadlock')
            return False

        total_sent += sent_bytes

    return True

# Lower Layer

def __receive(s: socket, total_bytes: int):

    #logging.debug(f'bout to read')

    data = b""
    total_received = 0

    # Recepción de los datos
    while total_received < total_bytes:
        to_receive = min(total_bytes - total_received, MAX_PACKET_SIZE)
        chunk = s.recv(to_receive)
        if len(chunk) == 0:
            raise ReadZeroException()  # No se recibió ningún dato, posible cierre de conexión
        data += chunk
        total_received += len(chunk)

    #logging.debug(f'action: __receive ')
    logging.debug(f'readed {len(data)}/{total_bytes}')
    return data


def __send(s: socket, buffer: bytes):
    #logging.debug(f'action: __send | buffer: {buffer}')
    sent = 0
    while sent < len(buffer):

        actual_sent = s.send(buffer[sent:])
        sent += actual_sent

        if actual_sent == 0:
            raise ShortWriteException()

    logging.debug(f'writed {sent}/{len(buffer)}')
    return sent
