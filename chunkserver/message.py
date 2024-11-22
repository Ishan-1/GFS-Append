# import socket
# import json
# # Define message object for communication between master, chunkserver, and client
# class Message_Manager:
#     message_types = ['REQUEST','RESPONSE','HEARTBEAT','DATA']
#     def send_message(self, socket, message_type, message_data):
#         if message_type not in self.message_types:
#             print("Invalid message type")
#             return False
#         message ={'Type':message_type, 'Data':message_data}
#         try:
#            socket.send(json.dumps(message).encode())
#         except:
#             print("Error sending message")
#             return False
#         return True
        
#     def receive_message(self, socket) :
#         message = socket.recv(1024).decode()
#         message = json.loads(message)
#         message_type = message['Type']
#         if message_type not in self.message_types:
#             print("Invalid message type")
#             return "",{}
#         message_data = message['Data']
#         return message_type, message_data


# message.py

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
