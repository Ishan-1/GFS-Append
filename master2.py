import socket
import threading
import time
import message
import math
import uuid
import sys
import os

# Ensure config is in the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import MASTER_SERVER_PORT, CHUNK_SIZE, NO_OF_REPLICAS

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
        self.append_transactions = {}  # Track append transactions

    def start(self):
        """Start the master server to listen for chunkservers and client requests."""
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        master_socket.bind((self.host, self.port))
        master_socket.listen(10)
        print(f"Master server started on {self.host}:{self.port}")
        
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()
        
        while True:
            try:
                client_socket, addr = master_socket.accept()
                threading.Thread(target=self.handle_connection, args=(client_socket,), daemon=True).start()
            except Exception as e:
                print(f"Error accepting connection: {e}")

    def handle_connection(self, conn):
        """Handle incoming connections from chunkservers or clients."""
        try:
            request_type, request_data = self.message_manager.receive_message(conn)
            
            if request_type == 'REGISTER':
                self.register_chunkserver(conn, request_data)
            elif request_type == 'REQUEST':
                self.handle_client_request(conn, request_data)
            elif request_type == 'CHUNK_DIRECTORY':
                self.update_chunk_directory(request_data)
            elif request_type == 'HEARTBEAT':
                self.handle_heartbeat(conn, request_data)
            else:
                print(f"Unknown request type: {request_type}")
        except Exception as e:
            print(f"Error handling connection: {e}")
        finally:
            conn.close()

    def register_chunkserver(self, conn, data):
        """Register a new chunkserver."""
        with self.lock:
            chunkserver_id = len(self.chunkservers) + 1
            chunkserver_address = data.get('Address')  # Expecting tuple [host, port]
            
            self.chunkservers[chunkserver_id] = {
                'socket': conn,
                'last_heartbeat': time.time(),
                'address': tuple(chunkserver_address)
            }
        
        print(f"Registered chunkserver {chunkserver_id} at {chunkserver_address}")
        
        # Send acknowledgment with chunkserver ID
        response = {'Status': 'SUCCESS', 'Chunkserver_ID': chunkserver_id}
        self.message_manager.send_message(conn, 'RESPONSE', response)

    def update_chunk_directory(self, data):
        """Update chunk directory from chunkserver."""
        chunk_directory = data.get('Chunk_Directory', {})
        with self.lock:
            for chunk_id, chunk_info in chunk_directory.items():
                # Update chunk locations
                self.chunk_locations[chunk_id] = chunk_info.get('Servers', [])
        print("Chunk directory updated from chunkserver")

    def handle_heartbeat(self, conn, data):
        """Handle heartbeat from chunkservers."""
        # Simply update the last heartbeat time
        with self.lock:
            for cs_id, cs_info in self.chunkservers.items():
                if cs_info['socket'] == conn:
                    cs_info['last_heartbeat'] = time.time()
                    break
        
        # Send heartbeat acknowledgment
        self.message_manager.send_message(conn, 'HEARTBEAT_ACK', {'Status': 'OK'})

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
                    # Remove chunks associated with this chunkserver
                    for chunk_id, servers in list(self.chunk_locations.items()):
                        if cs_id in servers:
                            servers.remove(cs_id)
                            if not servers:
                                del self.chunk_locations[chunk_id]

    def handle_client_request(self, conn, data):
        """Handle client operations: CREATE, DELETE, READ, APPEND."""
        operation = data.get('Operation')
        file_name = data.get('File_Name')
        
        if operation == 'CREATE':
            data_length = data.get('Data_Length', 0)
            self.handle_create(conn, file_name, data_length)
        elif operation == 'DELETE':
            self.handle_delete(conn, file_name)
        elif operation == 'READ':
            start_byte = data.get('Start_Byte', 0)
            end_byte = data.get('End_Byte', sys.maxsize)
            self.handle_read(conn, file_name, start_byte, end_byte)
        elif operation == 'APPEND':
            data_length = data.get('Data_Length', 0)
            self.handle_append(conn, file_name, data_length)
        else:
            response = {'Status': 'FAILED', 'Error': 'Unknown operation'}
            self.message_manager.send_message(conn, 'RESPONSE', response)

    def handle_create(self, conn, file_name, data_length):
        """Handle CREATE operation by splitting data into chunks and assigning replicas."""
        num_chunks = max(1, math.ceil(data_length / CHUNK_SIZE))
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
                chunks.append({
                    'Chunk_ID': chunk_id, 
                    'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in selected_servers]
                })
                
                # Assign chunk to file
                self.file_chunks.setdefault(file_name, []).append(chunk_id)
        
        response = {
            'Status': 'SUCCESS', 
            'Chunks': chunks, 
            'Replicas': self.replicas
        }
        self.message_manager.send_message(conn, 'RESPONSE', response)
        
        # Send chunk creation request to chunkservers
        for chunk in chunks:
            for chunkserver_address in chunk['Chunkservers']:
                try:
                    cs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    cs_socket.connect(chunkserver_address)
                    create_req = {
                        'Operation': 'CREATE',
                        'Chunk_ID': chunk['Chunk_ID'],
                        'File_Name': file_name
                    }
                    self.message_manager.send_message(cs_socket, 'REQUEST', create_req)
                    cs_socket.close()
                except Exception as e:
                    print(f"Error creating chunk {chunk['Chunk_ID']} on {chunkserver_address}: {e}")

    def handle_delete(self, conn, file_name):
        """Handle DELETE operation by removing all associated chunks."""
        with self.lock:
            chunk_ids = self.file_chunks.get(file_name, [])
            if not chunk_ids:
                response = {'Status': 'FAILED', 'Error': 'File not found'}
                self.message_manager.send_message(conn, 'RESPONSE', response)
                return
            
            # Delete chunks from chunkservers
            for chunk_id in chunk_ids:
                servers = self.chunk_locations.get(chunk_id, [])
                for cs_id in servers:
                    cs_address = self.chunkservers[cs_id]['address']
                    try:
                        cs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        cs_socket.connect(cs_address)
                        delete_req = {
                            'Operation': 'DELETE',
                            'Chunk_ID': chunk_id,
                            'File_Name': file_name
                        }
                        self.message_manager.send_message(cs_socket, 'REQUEST', delete_req)
                        cs_socket.close()
                    except Exception as e:
                        print(f"Error deleting chunk {chunk_id} on {cs_address}: {e}")
                
                # Remove chunk locations
                del self.chunk_locations[chunk_id]
            
            # Remove file entry
            del self.file_chunks[file_name]
        
        response = {
            'Status': 'SUCCESS', 
            'Message': f"File '{file_name}' deleted successfully."
        }
        self.message_manager.send_message(conn, 'RESPONSE', response)

    def handle_read(self, conn, file_name, start_byte, end_byte):
        """Handle READ operation by identifying relevant chunks."""
        with self.lock:
            chunk_ids = self.file_chunks.get(file_name, [])
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
            for chunk_id in relevant_chunks:
                chunks_info.append({
                    'Chunk_ID': chunk_id, 
                    'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in self.chunk_locations[chunk_id]]
                })
        
        response = {'Status': 'SUCCESS', 'Chunks': chunks_info}
        self.message_manager.send_message(conn, 'RESPONSE', response)

    def handle_append(self, conn, file_name, data_length):
        """Handle APPEND operation."""
        # Generate unique transaction ID
        transaction_id = str(uuid.uuid4())
        
        # Allocate chunks and select chunkservers
        num_chunks = max(1, math.ceil(data_length / CHUNK_SIZE))
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
                chunks.append({
                    'Chunk_ID': chunk_id, 
                    'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in selected_servers]
                })
            
            # Store transaction details
            self.append_transactions[transaction_id] = {
                'File_Name': file_name,
                'Chunks': chunks,
                'Status': 'PREPARE'
            }
        
        # Prepare phase: check chunkserver readiness
        prepare_tasks = []
        for chunk in chunks:
            for chunkserver_address in chunk['Chunkservers']:
                task = threading.Thread(target=self.send_prepare_to_chunkserver, 
                                        args=(chunkserver_address, transaction_id, chunk['Chunk_ID'], data_length))
                task.start()
                prepare_tasks.append(task)
        
        # Wait for all prepare tasks to complete
        for task in prepare_tasks:
            task.join()
        
        # Evaluate prepare phase results
        prepare_responses = self.append_transactions[transaction_id].get('Prepare_Responses', [])
        
        if all(resp.get('Status') == 'READY' for resp in prepare_responses):
            # Commit phase
            commit_tasks = []
            for chunk in chunks:
                for chunkserver_address in chunk['Chunkservers']:
                    task = threading.Thread(target=self.send_commit_to_chunkserver, 
                                            args=(chunkserver_address, transaction_id, chunk['Chunk_ID']))
                    task.start()
                    commit_tasks.append(task)
            
            # Wait for all commit tasks
            for task in commit_tasks:
                task.join()
            
            # Evaluate commit results
            commit_responses = self.append_transactions[transaction_id].get('Commit_Responses', [])
            
            if all(resp.get('Status') == 'SUCCESS' for resp in commit_responses):
                # Update file chunks
                with self.lock:
                    self.file_chunks.setdefault(file_name, []).extend(
                        chunk['Chunk_ID'] for chunk in chunks
                    )
                
                response = {
                    'Status': 'SUCCESS', 
                    'Chunks': chunks, 
                    'Replicas': self.replicas,
                    'Transaction_ID': transaction_id
                }
            else:
                # Abort transaction
                self.abort_append_transaction(transaction_id)
                response = {'Status': 'FAILED', 'Error': 'Commit failed'}
        else:
            # Abort transaction
            self.abort_append_transaction(transaction_id)
            response = {'Status': 'FAILED', 'Error': 'Prepare phase failed'}
        
        # Clean up transaction
        with self.lock:
            if transaction_id in self.append_transactions:
                del self.append_transactions[transaction_id]
        
        # Send response to client
        self.message_manager.send_message(conn, 'RESPONSE', response)

    def send_prepare_to_chunkserver(self, chunkserver_address, transaction_id, chunk_id, data_length):
        """Send PREPARE message to a specific chunkserver."""
        try:
            cs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cs_socket.connect(chunkserver_address)
            
            prepare_request = {
                'Operation': 'PREPARE',
                'Transaction_ID': transaction_id,
                'Chunk_ID': chunk_id,
                'Data_Length': data_length
            }
            self.message_manager.send_message(cs_socket, 'REQUEST', prepare_request)
            response_type, response_data = self.message_manager.receive_message(cs_socket)
            
            with self.lock:
                if transaction_id not in self.append_transactions:
                    self.append_transactions[transaction_id] = {'Prepare_Responses': []}
                self.append_transactions[transaction_id]['Prepare_Responses'].append(response_data)
            
            cs_socket.close()
        except Exception as e:
            print(f"Error in PREPARE phase for chunkserver {chunkserver_address}: {e}")

    def send_commit_to_chunkserver(self, chunkserver_address, transaction_id, chunk_id):
        """Send COMMIT message to a specific chunkserver."""
        try:
            cs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cs_socket.connect(chunkserver_address)
            
            commit_request = {
                'Operation': 'COMMIT',
                'Transaction_ID': transaction_id,
                'Chunk_ID': chunk_id
            }
            self.message_manager.send_message(cs_socket, 'REQUEST', commit_request)
            response_type, response_data = self.message_manager.receive_message(cs_socket)
            
            with self.lock:
                if transaction_id not in self.append_transactions:
                    self.append_transactions[transaction_id] = {'Commit_Responses': []}
                self.append_transactions[transaction_id]['Commit_Responses'].append(response_data)
            
            cs_socket.close()
        except Exception as e:
            print(f"Error in COMMIT phase for chunkserver {chunkserver_address}: {e}")

    def send_abort_to_chunkserver(self, chunkserver_address, transaction_id):
        """Send ABORT message to a specific chunkserver."""
        try:
            cs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cs_socket.connect(chunkserver_address)
            
            abort_request = {
                'Operation': 'ABORT',
                'Transaction_ID': transaction_id
            }
            self.message_manager.send_message(cs_socket, 'REQUEST', abort_request)
            response_type, response_data = self.message_manager.receive_message(cs_socket)
            
            print(f"Abort response from {chunkserver_address}: {response_data}")
            cs_socket.close()
        except Exception as e:
            print(f"Error in ABORT phase for chunkserver {chunkserver_address}: {e}")

    def abort_append_transaction(self, transaction_id):
        """Abort an ongoing append transaction."""
        with self.lock:
            transaction = self.append_transactions.get(transaction_id, {})
            if not transaction:
                return

            for chunk in transaction['Chunks']:
                for chunkserver_address in chunk['Chunkservers']:
                    self.send_abort_to_chunkserver(chunkserver_address, transaction_id)
                    
                    
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