import socket
import threading
import message  # Custom message manager module

class Client:
    def __init__(self, master_host='localhost', master_port=5000):
        self.master_host = master_host
        self.master_port = master_port
        self.message_manager = message.Message_Manager()
    
    def connect_to_master(self):
        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((self.master_host, self.master_port))
            return master_socket
        except ConnectionError:
            print("Error connecting to master server.")
            return None
    
    def get_chunk_location(self, operation, file_name, chunk_id, chunk_number=None):
        master_socket = self.connect_to_master()
        if master_socket:
            # Send request to master for chunk location details
            request_data = {
                'Operation': operation,
                'File_Name': file_name,
                'Chunk_ID': chunk_id,
                'Chunk_Number': chunk_number
            }
            self.message_manager.send_message(master_socket, 'REQUEST', request_data)
            response_type, response_data = self.message_manager.receive_message(master_socket)
            master_socket.close()
            
            if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
                return response_data['Chunkservers'], response_data.get('Primary')
            else:
                print("Failed to retrieve chunk location from master.")
                return None, None
        return None, None
    
    def perform_operation(self, operation, file_name, chunk_id, chunk_number=None, start_byte=None, end_byte=None, data=None):
        chunkservers, primary = self.get_chunk_location(operation, file_name, chunk_id, chunk_number)
        
        if chunkservers:
            for chunkserver_id in chunkservers:
                success = self.connect_to_chunkserver(chunkserver_id, operation, file_name, chunk_id, start_byte, end_byte, data)
                if success:
                    print(f"{operation} operation successful on chunkserver {chunkserver_id}.")
                    break
                else:
                    print(f"Chunkserver {chunkserver_id} failed. Trying next replica...")
            else:
                print(f"{operation} operation failed. All replicas unavailable.")
        else:
            print("No chunkservers available for the requested chunk.")
    
    def connect_to_chunkserver(self, chunkserver_id, operation, file_name, chunk_id, start_byte, end_byte, data):
        try:
            chunkserver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Assuming chunkserver_id corresponds to address
            chunkserver_socket.connect(chunkserver_id)
            # Send operation request to the chunkserver
            request_data = {
                'Operation': operation,
                'File_Name': file_name,
                'Chunk_ID': chunk_id,
                'Start_Byte': start_byte,
                'End_Byte': end_byte,
                'Data': data
            }
            self.message_manager.send_message(chunkserver_socket, 'REQUEST', request_data)
            response_type, response_data = self.message_manager.receive_message(chunkserver_socket)
            chunkserver_socket.close()
            
            return response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS'
        
        except ConnectionError:
            print(f"Failed to connect to chunkserver {chunkserver_id}")
            return False
    
    def cli_loop(self):
        while True:
            operation = input("Enter operation (CREATE, DELETE, READ, APPEND): ").strip().upper()
            file_name = input("Enter file name: ").strip()
            chunk_id = input("Enter chunk ID: ").strip()
            chunk_number = int(input("Enter chunk number: ").strip()) if operation in ['CREATE', 'APPEND'] else None
            
            if operation == 'READ':
                start_byte = int(input("Enter start byte: ").strip())
                end_byte = int(input("Enter end byte: ").strip())
                self.perform_operation(operation, file_name, chunk_id, start_byte=start_byte, end_byte=end_byte)
            elif operation == 'CREATE':
                self.perform_operation(operation, file_name, chunk_id, chunk_number=chunk_number)
            elif operation == 'DELETE':
                self.perform_operation(operation, file_name, chunk_id)
            elif operation == 'APPEND':
                data = input("Enter data to append: ").strip().encode()
                self.perform_operation(operation, file_name, chunk_id, chunk_number=chunk_number, data=data)
            else:
                print("Invalid operation. Try again.")

# Entry point for the client application
if __name__ == "__main__":
    client = Client()
    client.cli_loop()
