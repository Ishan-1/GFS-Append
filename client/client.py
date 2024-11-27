
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
        For READ, start_byte and end_byte specify the byte range.
        """
        master_socket = self.connect_to_master()
        if not master_socket:
            print("[ERROR] Failed to connect to master server.")
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

        self.message_manager.send_message(master_socket, 'REQUEST', request_data)

        response_type, response_data = self.message_manager.receive_message(master_socket)
        master_socket.close()

        if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
            # print(f"[DEBUG] Master server responded successfully: {response_data}")
            return response_data
        else:
            error_message = response_data.get('Error', 'Unknown error')
            print(f"[ERROR] Master server response: {error_message}")
            return None

    def fetch_data_from_chunkserver(self, server, operation, file_name, chunk_Number, start_byte, end_byte):
        """Fetch data from a chunkserver for READ operation."""
        try:
            chunkserver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chunkserver_socket.connect(tuple(server))  # server is a tuple (host, port)

            request_data = {
                'Operation': operation,
                'File_Name': file_name,
                'Chunk_Number': chunk_Number,
                'Start': start_byte,
                'End': end_byte
            }
            self.message_manager.send_message(chunkserver_socket, 'REQUEST', request_data)

            response_type, response_data = self.message_manager.receive_message(chunkserver_socket)
            chunkserver_socket.close()
            

            if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
                return response_data['Data'].encode('latin1')  # Decode back to bytes
            else:
                error_message = response_data.get('Error', 'Unknown error')
                print(f"[ERROR] READ failed on chunk {chunk_Number} from server {server}: {error_message}")
                return None

        except ConnectionError:
            print(f"[ERROR] Failed to connect to chunkserver {server} for READ operation.")
            return None


    def perform_read(self, file_name, start_byte, end_byte):
        """Handle the READ operation sequentially by contacting chunkservers."""
        
        # Get chunk locations from the master server
        response = self.get_chunk_locations('READ', file_name, start_byte=start_byte, end_byte=end_byte)
        if not response:
            print("[ERROR] READ operation failed. Unable to retrieve chunk locations.")
            return 
        
        chunks = response.get('Chunks', [])
        read_data = bytearray()

        # Process each chunk sequentially
        for chunk_info in chunks:
            chunk_Number = chunk_info['Chunk_Number']
            assigned_servers = chunk_info['Chunkservers']

            chunk_fetched = False  # Flag to track successful fetch
            for server in assigned_servers:
                data = self.fetch_data_from_chunkserver(server, 'READ', file_name, chunk_Number, start_byte, end_byte)
                if data is not None:
                    read_data.extend(data)
                    chunk_fetched = True
                    break  # Exit the loop once data is successfully fetched
                else:
                    print(f"[WARNING] Failed to read chunk {chunk_Number} from server {server}. Trying next replica...")

            if not chunk_fetched:
                print(f"[ERROR] Unable to fetch chunk {chunk_Number} from all replicas.")

        # Display the final data retrieved
        if read_data:
            print(f"READ operation for '{file_name}' completed. Data retrieved:")
            print(read_data.decode(errors='ignore'))
        else:
            print(f"[ERROR] READ operation for '{file_name}' failed. No data could be retrieved.")

    def perform_create(self, file_name, data):
        """Handle the CREATE operation."""
        master_socket = self.connect_to_master()
        if not master_socket:
            print("CREATE operation failed: Cannot connect to master server.")
            return

        request_data = {
            'Operation': 'CREATE',
            'File_Name': file_name,
            'Data': data.decode('latin1'),  # Encode bytes as string for transmission
            # 'Data_Length': len(data)
        }
        
        print(request_data)
        
        self.message_manager.send_message(master_socket, 'REQUEST', request_data)
        response_type, response_data = self.message_manager.receive_message(master_socket)
        master_socket.close()

        if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
            print(f"File '{file_name}' created successfully.")
        else:
            print(f"CREATE operation failed: {response_data.get('Error', 'Unknown error')}")

    def perform_delete(self, file_name):
        """Handle the DELETE operation."""
        master_socket = self.connect_to_master()
        if not master_socket:
            print("DELETE operation failed: Cannot connect to master server.")
            return

        request_data = {
            'Operation': 'DELETE',
            'File_Name': file_name
        }
        
        self.message_manager.send_message(master_socket, 'REQUEST', request_data)
        response_type, response_data = self.message_manager.receive_message(master_socket)
        master_socket.close()

        if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
            print(f"File '{file_name}' deleted successfully.")
        else:
            print(f"DELETE operation failed: {response_data.get('Error', 'Unknown error')}")
 
    
    def perform_append(self, file_name, data):
        """Handle the APPEND operation."""
        master_socket = self.connect_to_master()
        if not master_socket:
            print("[ERROR] APPEND operation failed: Cannot connect to master server.")
            return

        request_data = {
            'Operation': 'APPEND',
            'File_Name': file_name,
            'Data': data.decode('latin1'),  # Encode bytes as string for transmission
            'Data_Length': len(data)
        }
        
        self.message_manager.send_message(master_socket, 'REQUEST', request_data)
        response_type, response_data = self.message_manager.receive_message(master_socket)
        master_socket.close()
        # print(f"[DEBUG] response_data: ", response_data)
        if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
            # print(f"[DEBUG] APPEND operation successful: {response_data}")
            print(f"Data appended to '{file_name}' successfully.")
        elif response_type == 'RESPONSE' and response_data['Status'] == 'FAILED':
            # print(f"[DEBUG] APPEND operation successful: {response_data}")
            print(f"APPEND Aborted:- Error {response_data['Error']}.")
        else:
            error_message = response_data.get('Error', 'Unknown error')
            print(f"[ERROR] APPEND operation failed: {error_message}")

            
            
    
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
