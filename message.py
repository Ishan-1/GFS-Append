
import json
import struct

class Message_Manager:
    def send_message(self, sock, msg_type, data):
        """Serialize and send a message over a socket."""
        message = {
            'Type': msg_type,
            'Data': data
        }
        serialized = json.dumps(message).encode('utf-8')
        # Prefix each message with a 4-byte length (network byte order)
        sock.sendall(struct.pack('!I', len(serialized)) + serialized)
    
    def receive_message(self, sock):
        """Receive and deserialize a message from a socket."""
        # Read the message length (4 bytes)
        raw_msglen = self.recvall(sock, 4)
        if not raw_msglen:
            return None, None
        msglen = struct.unpack('!I', raw_msglen)[0]
        # Read the message data
        data = self.recvall(sock, msglen)
        if not data:
            return None, None
        message = json.loads(data.decode('utf-8'))
        return message.get('Type'), message.get('Data')
    
    def recvall(self, sock, n):
        """Helper function to receive n bytes or return None if EOF is hit."""
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data
