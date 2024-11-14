# import socket
# import threading
# import time
# import message  # Custom message manager module

# class MasterServer:
#     def __init__(self, host='localhost', port=5000):
#         self.host = host
#         self.port = port
#         self.chunkservers = {}  # Store metadata and health status of each chunkserver
#         self.chunk_locations = {}  # Track chunks and their assigned servers
#         self.primary_chunks = {}  # Track which chunkserver holds the primary role for each chunk
#         self.message_manager = message.Message_Manager()
    
#     def start(self):
#         # Start server socket to accept connections from chunkservers
#         master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         master_socket.bind((self.host, self.port))
#         master_socket.listen(5)
        
#         print("Master server started and listening for chunkservers...")
#         threading.Thread(target=self.heartbeat_monitor).start()
        
#         while True:
#             chunkserver_socket, addr = master_socket.accept()
#             threading.Thread(target=self.handle_chunkserver, args=(chunkserver_socket,)).start()
    
#     def handle_chunkserver(self, chunkserver_socket):
#         # Register the chunkserver and receive metadata
#         request_type, metadata = self.message_manager.receive_message(chunkserver_socket)
        
#         if request_type == 'REGISTER':
#             chunkserver_id = metadata['Chunkserver_ID']
#             self.chunkservers[chunkserver_id] = {
#                 'socket': chunkserver_socket,
#                 'last_heartbeat': time.time(),
#                 'chunks': metadata['Chunk_Directory']
#             }
#             print(f"Chunkserver {chunkserver_id} registered with metadata.")
            
#             # Store chunks in chunk_locations for easy lookup
#             for chunk_id in metadata['Chunk_Directory']:
#                 self.chunk_locations.setdefault(chunk_id, []).append(chunkserver_id)
    
#     def heartbeat_monitor(self):
#         # Periodically check chunkserver heartbeats
#         while True:
#             for chunkserver_id, info in list(self.chunkservers.items()):
#                 if time.time() - info['last_heartbeat'] > 10:
#                     print(f"Chunkserver {chunkserver_id} appears to be down.")
#                     del self.chunkservers[chunkserver_id]
#             time.sleep(5)
    
#     def handle_client_request(self, client_socket, request_type, request_data):
#         if request_type == 'CREATE':
#             self.handle_create(client_socket, request_data)
#         elif request_type == 'DELETE':
#             self.handle_delete(client_socket, request_data)
#         elif request_type == 'READ':
#             self.handle_read(client_socket, request_data)
#         elif request_type == 'APPEND':
#             self.handle_append(client_socket, request_data)
#         else:
#             response = {'Status': 'FAILED', 'Error': 'Invalid operation requested'}
#             self.message_manager.send_message(client_socket, 'RESPONSE', response)

#     def select_chunkservers(self):
#         # Simple round-robin or selection logic for demonstration
#         return list(self.chunkservers.keys())[:3]  # Select the first 3 chunkservers

#     # Define methods for each operation...
#     def handle_create(self, client_socket, request_data):
#         chunk_id = request_data['Chunk_ID']
#         file_name = request_data['File_Name']
#         chunk_number = request_data['Chunk_Number']
        
#         # Select chunkservers for storing the new chunk
#         selected_chunkservers = self.select_chunkservers()
#         primary_chunkserver = selected_chunkservers[0] if selected_chunkservers else None
        
#         if primary_chunkserver:
#             # Store the new chunk on selected chunkservers
#             for chunkserver_id in selected_chunkservers:
#                 self.message_manager.send_message(
#                     self.chunkservers[chunkserver_id]['socket'],
#                     'REQUEST',
#                     {
#                         'Operation': 'CREATE',
#                         'Chunk_ID': chunk_id,
#                         'File_Name': file_name,
#                         'Chunk_Number': chunk_number,
#                         'Primary': chunkserver_id == primary_chunkserver,
#                         'Data': b''  # Placeholder for initial data
#                     }
#                 )
                
#             # Update metadata
#             self.chunk_locations[chunk_id] = selected_chunkservers
#             self.primary_chunks[chunk_id] = primary_chunkserver
            
#             response = {
#                 'Status': 'SUCCESS',
#                 'Chunk_ID': chunk_id,
#                 'Chunkservers': selected_chunkservers,
#                 'Primary': primary_chunkserver
#             }
#         else:
#             response = {'Status': 'FAILED', 'Error': 'No chunkserver available for CREATE'}
        
#         self.message_manager.send_message(client_socket, 'RESPONSE', response)
        
        
#     def handle_delete(self, client_socket, request_data):
#         file_name = request_data['File_Name']
#         chunk_id = request_data['Chunk_ID']

#         if chunk_id in self.chunk_locations:
#             # Retrieve chunkservers storing this chunk
#             chunkservers = self.chunk_locations[chunk_id]
#             for chunkserver_id in chunkservers:
#                 # Send DELETE request to each chunkserver
#                 self.message_manager.send_message(
#                     self.chunkservers[chunkserver_id]['socket'],
#                     'REQUEST',
#                     {
#                         'Operation': 'DELETE',
#                         'Chunk_ID': chunk_id,
#                         'File_Name': file_name
#                     }
#                 )
#             # Update metadata after deletion
#             del self.chunk_locations[chunk_id]
#             del self.primary_chunks[chunk_id]

#             response = {
#                 'Status': 'SUCCESS',
#                 'Message': f"Deleted chunk {chunk_id} across all replicas"
#             }
#         else:
#             response = {
#                 'Status': 'FAILED',
#                 'Error': "Chunk ID not found"
#             }

#         self.message_manager.send_message(client_socket, 'RESPONSE', response)

#     def handle_read(self, client_socket, request_data):
#         file_name = request_data['File_Name']
#         chunk_id = request_data['Chunk_ID']

#         if chunk_id in self.chunk_locations:
#             # Get chunkservers and primary details
#             chunkservers = self.chunk_locations[chunk_id]
#             primary_chunkserver = self.primary_chunks.get(chunk_id)
#             response = {
#                 'Status': 'SUCCESS',
#                 'Chunkservers': chunkservers,
#                 'Primary': primary_chunkserver
#             }
#         else:
#             response = {
#                 'Status': 'FAILED',
#                 'Error': "Chunk ID not found"
#             }

#         self.message_manager.send_message(client_socket, 'RESPONSE', response)

#     def handle_append(self, client_socket, request_data):
#         file_name = request_data['File_Name']
#         chunk_id = request_data['Chunk_ID']
#         data = request_data['Data']

#         if chunk_id in self.chunk_locations:
#             chunkservers = self.chunk_locations[chunk_id]
#             primary_chunkserver = self.primary_chunks.get(chunk_id)
            
#             # First, send APPEND request to the primary chunkserver
#             success = False
#             for chunkserver_id in chunkservers:
#                 is_primary = (chunkserver_id == primary_chunkserver)
#                 self.message_manager.send_message(
#                     self.chunkservers[chunkserver_id]['socket'],
#                     'REQUEST',
#                     {
#                         'Operation': 'APPEND',
#                         'Chunk_ID': chunk_id,
#                         'File_Name': file_name,
#                         'Data': data,
#                         'Primary': is_primary
#                     }
#                 )
#                 # Await response and confirm success from primary chunkserver
#                 response_type, response_data = self.message_manager.receive_message(self.chunkservers[chunkserver_id]['socket'])
#                 if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS' and is_primary:
#                     success = True
#                     break
            
#             if success:
#                 response = {
#                     'Status': 'SUCCESS',
#                     'Chunkservers': chunkservers,
#                     'Primary': primary_chunkserver
#                 }
#             else:
#                 response = {
#                     'Status': 'FAILED',
#                     'Error': "Primary chunkserver failed to append"
#                 }
#         else:
#             response = {
#                 'Status': 'FAILED',
#                 'Error': "Chunk ID not found"
#             }

#         self.message_manager.send_message(client_socket, 'RESPONSE', response)


#     def handle_heartbeat(self, chunkserver_id):
#         self.chunkservers[chunkserver_id]['last_heartbeat'] = time.time()
#         print(f"Heartbeat received from chunkserver {chunkserver_id}.")


# if __name__ == "__main__":
#     master_server = MasterServer()
#     master_server.start()


import socket
import threading
import time
import message  # Custom message manager module
import json
import math

CHUNK_SIZE = 256  # Define chunk size in bytes

class MasterServer:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.chunkservers = {}  # key: chunkserver_id, value: {'socket': socket, 'last_heartbeat': timestamp, 'address': (host, port)}
        self.chunk_locations = {}  # key: chunk_id, value: list of chunkserver_ids
        self.file_chunks = {}  # key: file_name, value: list of chunk_ids
        self.next_chunk_id = 1
        self.replicas = 3  # Number of replicas per chunk
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
