
import socket
import threading
import sys
import os
import math
import json
from queue import Queue

# Adjust the path as needed to import the message module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import message  # Custom message manager module
from config import MASTER_SERVER_PORT,CHUNK_SIZE,NO_OF_REPLICAS




class Client:
    def __init__(self, master_host='localhost', master_port = MASTER_SERVER_PORT):
        self.master_host = master_host
        self.master_port = master_port
        self.message_manager = message.Message_Manager()
    
    def connect_to_master(self):
        """Establish a connection to the master server."""
        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((self.master_host, self.master_port))
            return master_socket
        except ConnectionError:
            print("Error connecting to master server.")
            return None
    
    def get_chunk_locations(self, operation, file_name, data_length=None, start_byte=None, end_byte=None):
        """
        Request chunk locations from the master server based on the operation.
        For CREATE and APPEND, data_length is required to determine how many chunks are needed.
        For READ, start_byte and end_byte specify the byte range.
        For DELETE, only the file_name is needed.
        """
        master_socket = self.connect_to_master()
        if not master_socket:
            return None

        # Prepare request data based on operation
        request_data = {
            'Operation': operation,
            'File_Name': file_name
        }
        
        if operation in ['CREATE', 'APPEND'] and data_length is not None:
            request_data['Data_Length'] = data_length
        elif operation == 'READ':
            request_data['Start_Byte'] = start_byte
            request_data['End_Byte'] = end_byte
        
        # Send request to master
        self.message_manager.send_message(master_socket, 'REQUEST', request_data)
        response_type, response_data = self.message_manager.receive_message(master_socket)
        master_socket.close()
        
        if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
            return response_data
        else:
            print(f"Master server response: {response_data.get('Error', 'Unknown error')}")
            return None
    
    def perform_create(self, file_name, data):
        """Handle the CREATE operation."""
        data_length = len(data)
        response = self.get_chunk_locations('CREATE', file_name, data_length=data_length)
        if not response:
            print("CREATE operation failed Master server Not responding.")
            return
        
        chunks = response.get('Chunks', [])
        replicas = response.get('Replicas', NO_OF_REPLICAS)  
        # Split data into chunks
        num_chunks = math.ceil(data_length / CHUNK_SIZE)
        data_chunks = [data[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE] for i in range(num_chunks)]
        
        # Assign each chunk to chunkservers
        for idx, chunk_data in enumerate(data_chunks):
            chunk_info = chunks[idx]
            chunk_id = chunk_info['Chunk_ID']
            assigned_servers = chunk_info['Chunkservers']
            
            # Send data to all replicas
            for server in assigned_servers:
                threading.Thread(target=self.send_data_to_chunkserver, args=(server, 'CREATE', file_name, chunk_id, chunk_data)).start()
        
        print(f"CREATE operation for '{file_name}' initiated.")
    
    def perform_delete(self, file_name):
        """Handle the DELETE operation."""
        response = self.get_chunk_locations('DELETE', file_name)
        if not response:
            print("DELETE operation failed.")
            return
        
        chunks = response.get('Chunks', [])
        
        # Send delete request to all chunkservers holding each chunk
        for chunk_info in chunks:
            chunk_id = chunk_info['Chunk_ID']
            assigned_servers = chunk_info['Chunkservers']
            
            for server in assigned_servers:
                threading.Thread(target=self.send_delete_operation_to_chunkserver, args=(server, 'DELETE', file_name, chunk_id)).start()
        
        print(f"DELETE operation for '{file_name}' initiated.")
    
    def perform_read(self, file_name, start_byte, end_byte):
        """Handle the READ operation."""
        response = self.get_chunk_locations('READ', file_name, start_byte=start_byte, end_byte=end_byte)
        if not response:
            print("READ operation failed.")
            return
        
        chunks = response.get('Chunks', [])
        read_data = bytearray()
        read_queue = Queue()
        
        # Fetch data from all relevant chunks
        for chunk_info in chunks:
            chunk_id = chunk_info['Chunk_ID']
            assigned_servers = chunk_info['Chunkservers']
            read_queue.put((chunk_id, assigned_servers))
        
        # Worker function to read from chunkservers
        def read_worker():
            while not read_queue.empty():
                chunk_id, servers = read_queue.get()
                for server in servers:
                    data = self.fetch_data_from_chunkserver(server, 'READ', file_name, chunk_id, start_byte, end_byte)
                    if data is not None:
                        read_data.extend(data)
                        break
                    else:
                        print(f"Failed to read chunk {chunk_id} from server {server}. Trying next replica...")
                read_queue.task_done()
        
        # Start worker threads
        num_threads = min(5, len(chunks))  # Limit number of threads
        for _ in range(num_threads):
            threading.Thread(target=read_worker).start()
        
        read_queue.join()
        
        print(f"READ operation for '{file_name}' completed. Data:")
        print(read_data.decode(errors='ignore'))
    
    def perform_append(self, file_name, data):
        """Handle the APPEND operation."""
        data_length = len(data)
        response = self.get_chunk_locations('APPEND', file_name, data_length=data_length)
        if not response:
            print("APPEND operation failed.")
            return
        
        chunks = response.get('Chunks', [])
        replicas = response.get('Replicas', NO_OF_REPLICAS)  

        # Split data into chunks
        num_chunks = math.ceil(data_length / CHUNK_SIZE)
        data_chunks = [data[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE] for i in range(num_chunks)]
        
        # Assign each chunk to chunkservers
        for idx, chunk_data in enumerate(data_chunks):
            chunk_info = chunks[idx]
            chunk_id = chunk_info['Chunk_ID']
            assigned_servers = chunk_info['Chunkservers']
            
            # Send data to all replicas
            for server in assigned_servers:
                threading.Thread(target=self.send_data_to_chunkserver, args=(server, 'APPEND', file_name, chunk_id, chunk_data)).start()
        
        print(f"APPEND operation for '{file_name}' initiated.")
    
    def send_data_to_chunkserver(self, server, operation, file_name, chunk_id, data):
        """Send CREATE or APPEND data to a chunkserver."""
        try:
            chunkserver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chunkserver_socket.connect(tuple(server))  # server is a tuple (host, port)
            
            request_data = {
                'Operation': operation,
                'File_Name': file_name,
                'Chunk_ID': chunk_id,
                'Data': data.decode('latin1')  # Encode bytes as string for transmission
            }
            self.message_manager.send_message(chunkserver_socket, 'REQUEST', request_data)
            response_type, response_data = self.message_manager.receive_message(chunkserver_socket)
            chunkserver_socket.close()
            
            if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
                print(f"{operation} on chunk {chunk_id} successful on {server}.")
            else:
                print(f"{operation} on chunk {chunk_id} failed on {server}: {response_data.get('Error', 'Unknown error')}")
        
        except ConnectionError:
            print(f"Failed to connect to chunkserver {server} for {operation} operation.")
    
    def send_delete_operation_to_chunkserver(self, server, operation, file_name, chunk_id):
        """Send DELETE operation to a chunkserver."""
        try:
            chunkserver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chunkserver_socket.connect(tuple(server))  # server is a tuple (host, port)
            
            request_data = {
                'Operation': operation,
                'File_Name': file_name,
                'Chunk_ID': chunk_id
            }
            self.message_manager.send_message(chunkserver_socket, 'REQUEST', request_data)
            response_type, response_data = self.message_manager.receive_message(chunkserver_socket)
            chunkserver_socket.close()
            
            if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
                print(f"{operation} on chunk {chunk_id} successful on {server}.")
            else:
                print(f"{operation} on chunk {chunk_id} failed on {server}: {response_data.get('Error', 'Unknown error')}")
        
        except ConnectionError:
            print(f"Failed to connect to chunkserver {server} for {operation} operation.")
    
    def fetch_data_from_chunkserver(self, server, operation, file_name, chunk_id, start_byte, end_byte):
        """Fetch data from a chunkserver for READ operation."""
        try:
            chunkserver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chunkserver_socket.connect(tuple(server))  # server is a tuple (host, port)
            
            request_data = {
                'Operation': operation,
                'File_Name': file_name,
                'Chunk_ID': chunk_id,
                'Start_Byte': start_byte,
                'End_Byte': end_byte
            }
            self.message_manager.send_message(chunkserver_socket, 'REQUEST', request_data)
            response_type, response_data = self.message_manager.receive_message(chunkserver_socket)
            chunkserver_socket.close()
            
            if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
                return response_data['Data'].encode('latin1')  # Decode back to bytes
            else:
                print(f"READ on chunk {chunk_id} failed on {server}: {response_data.get('Error', 'Unknown error')}")
                return None
        
        except ConnectionError:
            print(f"Failed to connect to chunkserver {server} for READ operation.")
            return None
    
    def cli_loop(self):
        """Command-line interface loop for user to specify operations and file details."""
        while True:
            print("\nAvailable operations: CREATE, DELETE, READ, APPEND, EXIT")
            operation = input("Enter operation: ").strip().upper()
            if operation == 'EXIT':
                print("Exiting client.")
                break
            file_name = input("Enter file name: ").strip()
            
            if operation == 'CREATE':
                data = input("Enter data to create the file: ").strip().encode()
                self.perform_create(file_name, data)
            elif operation == 'DELETE':
                confirm = input(f"Are you sure you want to delete '{file_name}'? (yes/no): ").strip().lower()
                if confirm == 'yes':
                    self.perform_delete(file_name)
                else:
                    print("DELETE operation canceled.")
            elif operation == 'READ':
                try:
                    start_byte = int(input("Enter start byte: ").strip())
                    end_byte = int(input("Enter end byte: ").strip())
                    if start_byte < 0 or end_byte < start_byte:
                        print("Invalid byte range.")
                        continue
                    self.perform_read(file_name, start_byte, end_byte)
                except ValueError:
                    print("Invalid input for byte ranges. Please enter integers.")
            elif operation == 'APPEND':
                data = input("Enter data to append: ").strip().encode()
                if not data:
                    print("No data entered for append.")
                    continue
                self.perform_append(file_name, data)
            else:
                print("Invalid operation. Try again.")

# Entry point for the client application
if __name__ == "__main__":
    client = Client()
    client.cli_loop()
