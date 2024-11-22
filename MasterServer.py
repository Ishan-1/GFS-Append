


import socket
import threading
import time
import message  # Custom message manager module
import json
import math
import os
import sys

# Adjust the path as needed to import the message module
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import message  # Custom message manager module
from config import MASTER_SERVER_PORT,CHUNK_SIZE,NO_OF_REPLICAS



class MasterServer:
    def __init__(self, host='localhost', port=MASTER_SERVER_PORT):
        self.host = host
        self.port = port
        self.chunkservers = {}  # key: chunkserver_id, value: {'socket': socket, 'last_heartbeat': timestamp, 'address': (host, port)}
        self.chunk_locations = {}  # key: chunk_id, value: list of chunkserver_ids
        self.file_chunks = {}  # key: file_name, value: list of chunk_ids
        self.next_chunk_id = 1
        self.replicas = NO_OF_REPLICAS  # Number of replicas per chunk
        self.message_manager = message.Message_Manager()
        self.lock = threading.Lock()  # To synchronize access to shared resources
    
    def start(self):
        """Start the master server to listen for chunkservers and client requests."""
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.bind((self.host, self.port))
        master_socket.listen(10)
        print(f"Master server started on {self.host}:{self.port}")
        
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()
        
        while True:
            client_socket, addr = master_socket.accept()
            threading.Thread(target=self.handle_connection, args=(client_socket,), daemon=True).start()
    
    def handle_connection(self, conn):
        """Handle incoming connections from chunkservers or clients."""
        try:
            request_type, request_data = self.message_manager.receive_message(conn)
            if request_type == 'REGISTER':
                self.register_chunkserver(conn, request_data)
            elif request_type == 'REQUEST':
                self.handle_client_request(conn, request_data)
            else:
                print(f"Unknown request type: {request_type}")
        except Exception as e:
            print(f"Error handling connection: {e}")
        finally:
            conn.close()
    
    def register_chunkserver(self, conn, data):
        """Register a new chunkserver."""
        chunkserver_id = self.next_chunk_id
        self.next_chunk_id += 1
        chunkserver_address = data.get('Address')  # Expecting tuple [host, port]
        with self.lock:
            self.chunkservers[chunkserver_id] = {
                'socket': conn,
                'last_heartbeat': time.time(),
                'address': tuple(chunkserver_address)
            }
        print(f"Registered chunkserver {chunkserver_id} at {chunkserver_address}")
        
        # Send acknowledgment with chunkserver ID
        response = {'Status': 'SUCCESS', 'Chunkserver_ID': chunkserver_id}
        self.message_manager.send_message(conn, 'RESPONSE', response)
    
    def heartbeat_monitor(self):
        """Monitor heartbeats from chunkservers to detect failures."""
        while True:
            time.sleep(5)
            current_time = time.time()
            with self.lock:
                to_remove = []
                for cs_id, info in self.chunkservers.items():
                    if current_time - info['last_heartbeat'] > 15:  # 15 seconds timeout
                        print(f"Chunkserver {cs_id} timed out.")
                        to_remove.append(cs_id)
                for cs_id in to_remove:
                    del self.chunkservers[cs_id]
                    # Handle reassignment of chunks if necessary
                    # For simplicity, not implemented here
            # Heartbeat checks can be enhanced as needed
    
    def handle_client_request(self, conn, data):
        """Handle client operations: CREATE, DELETE, READ, APPEND."""
        operation = data.get('Operation')
        file_name = data.get('File_Name')
        
        if operation == 'CREATE':
            data_length = data.get('Data_Length')
            self.handle_create(conn, file_name, data_length)
        elif operation == 'DELETE':
            self.handle_delete(conn, file_name)
        elif operation == 'READ':
            start_byte = data.get('Start_Byte')
            end_byte = data.get('End_Byte')
            self.handle_read(conn, file_name, start_byte, end_byte)
        elif operation == 'APPEND':
            data_length = data.get('Data_Length')
            self.handle_append(conn, file_name, data_length)
        else:
            response = {'Status': 'FAILED', 'Error': 'Unknown operation'}
            self.message_manager.send_message(conn, 'RESPONSE', response)
    
    def handle_create(self, conn, file_name, data_length):
        """Handle CREATE operation by splitting data into chunks and assigning replicas."""
        num_chunks = math.ceil(data_length / CHUNK_SIZE)
        chunks = []
        with self.lock:
            for _ in range(num_chunks):
                chunk_id = self.next_chunk_id
                self.next_chunk_id += 1
                # Select chunkservers for replicas
                selected_servers = self.select_chunkservers()
                if not selected_servers:
                    response = {'Status': 'FAILED', 'Error': 'Insufficient chunkservers available'}
                    self.message_manager.send_message(conn, 'RESPONSE', response)
                    return
                self.chunk_locations[chunk_id] = selected_servers
                chunks.append({'Chunk_ID': chunk_id, 'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in selected_servers]})
                # Assign chunk to file
                self.file_chunks.setdefault(file_name, []).append(chunk_id)
        
        response = {'Status': 'SUCCESS', 'Chunks': chunks, 'Replicas': self.replicas}
        self.message_manager.send_message(conn, 'RESPONSE', response)
    
    def handle_delete(self, conn, file_name):
        """Handle DELETE operation by removing all associated chunks."""
        with self.lock:
            chunk_ids = self.file_chunks.get(file_name)
            if not chunk_ids:
                response = {'Status': 'FAILED', 'Error': 'File not found'}
                self.message_manager.send_message(conn, 'RESPONSE', response)
                return
            
            for chunk_id in chunk_ids:
                servers = self.chunk_locations.get(chunk_id, [])
                # Notify all chunkservers to delete the chunk
                for cs_id in servers:
                    server_address = self.chunkservers[cs_id]['address']
                    threading.Thread(target=self.send_delete_to_chunkserver, args=(server_address, file_name, chunk_id)).start()
                # Remove chunk location
                del self.chunk_locations[chunk_id]
            # Remove file entry
            del self.file_chunks[file_name]
        
        response = {'Status': 'SUCCESS', 'Message': f"File '{file_name}' deleted successfully."}
        self.message_manager.send_message(conn, 'RESPONSE', response)
    
    def handle_read(self, conn, file_name, start_byte, end_byte):
        """Handle READ operation by identifying relevant chunks."""
        with self.lock:
            chunk_ids = self.file_chunks.get(file_name)
            if not chunk_ids:
                response = {'Status': 'FAILED', 'Error': 'File not found'}
                self.message_manager.send_message(conn, 'RESPONSE', response)
                return
            
            total_size = len(chunk_ids) * CHUNK_SIZE
            if start_byte >= total_size:
                response = {'Status': 'FAILED', 'Error': 'Start byte exceeds file size'}
                self.message_manager.send_message(conn, 'RESPONSE', response)
                return
            end_byte = min(end_byte, total_size - 1)
            # Determine which chunks cover the byte range
            start_chunk = start_byte // CHUNK_SIZE
            end_chunk = end_byte // CHUNK_SIZE
            relevant_chunks = chunk_ids[start_chunk:end_chunk + 1]
            chunks_info = []
            for idx, chunk_id in enumerate(relevant_chunks):
                chunks_info.append({'Chunk_ID': chunk_id, 'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in self.chunk_locations[chunk_id]]})
        
        response = {'Status': 'SUCCESS', 'Chunks': chunks_info}
        self.message_manager.send_message(conn, 'RESPONSE', response)
    
    def handle_append(self, conn, file_name, data_length):
        """Handle APPEND operation by assigning new chunks and replicas."""
        num_chunks = math.ceil(data_length / CHUNK_SIZE)
        chunks = []
        with self.lock:
            for _ in range(num_chunks):
                chunk_id = self.next_chunk_id
                self.next_chunk_id += 1
                # Select chunkservers for replicas
                selected_servers = self.select_chunkservers()
                if not selected_servers:
                    response = {'Status': 'FAILED', 'Error': 'Insufficient chunkservers available'}
                    self.message_manager.send_message(conn, 'RESPONSE', response)
                    return
                self.chunk_locations[chunk_id] = selected_servers
                chunks.append({'Chunk_ID': chunk_id, 'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in selected_servers]})
                # Assign chunk to file
                self.file_chunks.setdefault(file_name, []).append(chunk_id)
        
        response = {'Status': 'SUCCESS', 'Chunks': chunks, 'Replicas': self.replicas}
        self.message_manager.send_message(conn, 'RESPONSE', response)
    
    def select_chunkservers(self):
        """Select chunkservers for replicas using round-robin selection."""
        selected = []
        with self.lock:
            available_ids = list(self.chunkservers.keys())
            if len(available_ids) < self.replicas:
                return None  # Not enough chunkservers
            # Simple selection: first N available chunkservers
            selected = available_ids[:self.replicas]
        return selected
    
    def send_delete_to_chunkserver(self, server_address, file_name, chunk_id):
        """Send a DELETE request to a specific chunkserver."""
        try:
            cs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cs_socket.connect(tuple(server_address))
            
            request_data = {
                'Operation': 'DELETE',
                'File_Name': file_name,
                'Chunk_ID': chunk_id
            }
            self.message_manager.send_message(cs_socket, 'REQUEST', request_data)
            response_type, response_data = self.message_manager.receive_message(cs_socket)
            cs_socket.close()
            
            if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS':
                print(f"Chunk {chunk_id} deleted successfully on {server_address}.")
            else:
                print(f"Failed to delete chunk {chunk_id} on {server_address}: {response_data.get('Error', 'Unknown error')}")
        
        except ConnectionError:
            print(f"Failed to connect to chunkserver {server_address} for DELETE operation.")

# Entry point for the master server application
if __name__ == "__main__":
    master_server = MasterServer()
    master_server.start()
