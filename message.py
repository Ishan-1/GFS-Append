import socket
import json
# Define message object for communication between master, chunkserver, and client
class Message_Manager:
    message_types = ['REGISTER','HEARTBEAT','CREATE','APPEND','READ','DELETE']
    def send_message(self, socket, message_type, message_data):
        if message_type not in self.message_types:
            print("Invalid message type")
            return False
        message ={'Type':message_type, 'Data':message_data}
        try:
           socket.send(json.dumps(message).encode())
        except:
            print("Error sending message")
            return False
        return True
        
    def receive_message(self,socket):
        message = socket.recv(1024).decode()
        message = json.loads(message)
        message_type = message['Type']
        if message_type not in self.message_types:
            print("Invalid message type")
            return None,None
        message_data = message['Data']
        return message_type, message_data

