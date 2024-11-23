import socket
import threading
import time
import message
import math
import uuid
import sys
import os
import queue

# Ensure config is in the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import MASTER_SERVER_PORT, CHUNK_SIZE, NO_OF_REPLICAS

class MasterServer:
    def __init__(self, host='localhost', port=MASTER_SERVER_PORT):
        self.host = host
        self.port = port
        self.chunkservers = {}  # key: chunkserver_id, value: {'connection': conn, 'last_heartbeat': timestamp, 'address': (host, port), 'message_queue': Queue}
        self.chunk_locations = {}  # key: chunk_id, value: list of chunkserver_ids
        self.file_chunks = {}  # key: file_name, value: list of chunk_ids
        self.next_chunk_id = 1
        self.replicas = NO_OF_REPLICAS
        self.message_manager = message.Message_Manager()
        self.lock = threading.Lock()
        self.append_transactions = {}

    def start(self):
        """Start the master server to listen for connections."""
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        master_socket.bind((self.host, self.port))
        master_socket.listen(10)
        print(f"Master server started on {self.host}:{self.port}")
        
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()
        
        while True:
            try:
                client_socket, addr = master_socket.accept()
                threading.Thread(target=self.handle_initial_connection, args=(client_socket,), daemon=True).start()
            except Exception as e:
                print(f"Error accepting connection: {e}")

    def handle_initial_connection(self, conn):
        """Handle initial connection and determine if it's a chunkserver or client."""
        try:
            request_type, request_data = self.message_manager.receive_message(conn)
            
            if request_type == 'REGISTER':
                self.register_chunkserver(conn, request_data)
            else:
                # Handle client request and close connection afterward
                self.handle_client_request(conn, request_data)
                conn.close()
        except Exception as e:
            print(f"Error handling initial connection: {e}")
            conn.close()

    def register_chunkserver(self, conn, data):
        """Register a new chunkserver and start its message handling thread."""
        with self.lock:
            chunkserver_id = len(self.chunkservers) + 1
            chunkserver_address = tuple(data.get('Address'))
            
            # Create message queue for this chunkserver
            message_queue = queue.Queue()
            
            self.chunkservers[chunkserver_id] = {
                'connection': conn,
                'last_heartbeat': time.time(),
                'address': chunkserver_address,
                'message_queue': message_queue
            }
        
        print(f"Registered chunkserver {chunkserver_id} at {chunkserver_address}")
        
        # Send acknowledgment with chunkserver ID
        response = {'Status': 'SUCCESS', 'Chunkserver_ID': chunkserver_id}
        self.message_manager.send_message(conn, 'RESPONSE', response)
        
        # Start dedicated threads for this chunkserver
        threading.Thread(target=self.handle_chunkserver_messages, 
                       args=(chunkserver_id, conn), 
                       daemon=True).start()
        threading.Thread(target=self.process_chunkserver_queue, 
                       args=(chunkserver_id,), 
                       daemon=True).start()

    def handle_chunkserver_messages(self, chunkserver_id, conn):
        """Continuously handle messages from a specific chunkserver."""
        while True:
            try:
                request_type, request_data = self.message_manager.receive_message(conn)
                print("Received message from Chunkserver ",chunkserver_id)
                if request_type == 'HEARTBEAT':
                    print("Heartbeat")
                    self.handle_heartbeat(chunkserver_id)
                elif request_type == 'CHUNK_DIRECTORY':
                    self.update_chunk_directory(request_data)
                elif request_type == 'RESPONSE':
                    self.handle_chunkserver_response(chunkserver_id, request_data)
                else:
                    print(f"Unknown message type from chunkserver {chunkserver_id}: {request_type}")
                
            except Exception as e:
                print(f"Error handling messages from chunkserver {chunkserver_id}: {e}")
                self.handle_chunkserver_failure(chunkserver_id)
                break

    def handle_chunkserver_response(self, chunkserver_id, response_data):
        """Handle responses from chunkservers for operations."""
        transaction_id = response_data.get('Transaction_ID')
        if not transaction_id:
            return

        with self.lock:
            if transaction_id in self.append_transactions:
                transaction = self.append_transactions[transaction_id]
                
                if response_data.get('Operation') == 'PREPARE':
                    if 'Prepare_Responses' not in transaction:
                        transaction['Prepare_Responses'] = []
                    transaction['Prepare_Responses'].append(response_data)
                
                elif response_data.get('Operation') == 'COMMIT':
                    if 'Commit_Responses' not in transaction:
                        transaction['Commit_Responses'] = []
                    transaction['Commit_Responses'].append(response_data)

    def process_chunkserver_queue(self, chunkserver_id):
        """Process messages in the chunkserver's queue."""
        while True:
            try:
                with self.lock:
                    if chunkserver_id not in self.chunkservers:
                        break
                    chunkserver = self.chunkservers[chunkserver_id]
                
                message = chunkserver['message_queue'].get()
                try:
                    self.message_manager.send_message(chunkserver['connection'], 
                                                    message['type'], 
                                                    message['data'])
                except Exception as e:
                    print(f"Error sending message to chunkserver {chunkserver_id}: {e}")
                    self.handle_chunkserver_failure(chunkserver_id)
                    break
                
            except queue.Empty:
                time.sleep(0.1)
            except Exception as e:
                print(f"Error processing queue for chunkserver {chunkserver_id}: {e}")
                break

    def send_to_chunkserver(self, chunkserver_id, message_type, message_data):
        """Queue a message to be sent to a chunkserver."""
        with self.lock:
            if chunkserver_id in self.chunkservers:
                self.chunkservers[chunkserver_id]['message_queue'].put({
                    'type': message_type,
                    'data': message_data
                })

    def handle_heartbeat(self, chunkserver_id):
        """Update heartbeat timestamp for a chunkserver."""
        if chunkserver_id in self.chunkservers:
                self.chunkservers[chunkserver_id]['last_heartbeat'] = time.time()
                self.send_to_chunkserver(chunkserver_id, 'HEARTBEAT_ACK', {'Status': 'OK'})
                print("Heartbeat from chunkserver",chunkserver_id)

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
                    self.handle_chunkserver_failure(cs_id)

    def handle_chunkserver_failure(self, chunkserver_id):
        """Handle chunkserver failure by cleaning up its resources."""
        with self.lock:
            if chunkserver_id in self.chunkservers:
                try:
                    self.chunkservers[chunkserver_id]['connection'].close()
                except:
                    pass
                del self.chunkservers[chunkserver_id]
                
                # Remove chunks associated with this chunkserver
                for chunk_id, servers in list(self.chunk_locations.items()):
                    if chunkserver_id in servers:
                        servers.remove(chunkserver_id)
                        if not servers:
                            del self.chunk_locations[chunk_id]
                
                print(f"Chunkserver {chunkserver_id} removed due to failure")

    def update_chunk_directory(self, data):
        """Update chunk directory from chunkserver."""
        chunk_directory = data.get('Chunk_Directory', {})
        with self.lock:
            for chunk_id, chunk_info in chunk_directory.items():
                self.chunk_locations[chunk_id] = chunk_info.get('Servers', [])
        print("Chunk directory updated from chunkserver")

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
        """Handle CREATE operation."""
        num_chunks = max(1, math.ceil(data_length / CHUNK_SIZE))
        chunks = []
        
        with self.lock:
            for _ in range(num_chunks):
                chunk_id = self.next_chunk_id
                self.next_chunk_id += 1
                
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
                
                # Send create request to selected chunkservers
                for cs_id in selected_servers:
                    create_req = {
                        'Operation': 'CREATE',
                        'Chunk_ID': chunk_id,
                        'File_Name': file_name
                    }
                    self.send_to_chunkserver(cs_id, 'REQUEST', create_req)
                
                self.file_chunks.setdefault(file_name, []).append(chunk_id)
        
        response = {
            'Status': 'SUCCESS',
            'Chunks': chunks,
            'Replicas': self.replicas
        }
        self.message_manager.send_message(conn, 'RESPONSE', response)

    def handle_delete(self, conn, file_name):
        """Handle DELETE operation."""
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
                    delete_req = {
                        'Operation': 'DELETE',
                        'Chunk_ID': chunk_id,
                        'File_Name': file_name
                    }
                    self.send_to_chunkserver(cs_id, 'REQUEST', delete_req)
                
                del self.chunk_locations[chunk_id]
            
            del self.file_chunks[file_name]
        
        response = {'Status': 'SUCCESS', 'Message': f"File '{file_name}' deleted successfully"}
        self.message_manager.send_message(conn, 'RESPONSE', response)

    def handle_read(self, conn, file_name, start_byte, end_byte):
        """Handle READ operation."""
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

    def send_prepare_to_chunkserver(self, chunkserver_id, transaction_id, chunk_id, data_length):
        """Send PREPARE message to a specific chunkserver using persistent connection."""
        prepare_request = {
            'Operation': 'PREPARE',
            'Transaction_ID': transaction_id,
            'Chunk_ID': chunk_id,
            'Data_Length': data_length
        }
        self.send_to_chunkserver(chunkserver_id, 'REQUEST', prepare_request)

    def send_commit_to_chunkserver(self, chunkserver_id, transaction_id, chunk_id):
        """Send COMMIT message to a specific chunkserver using persistent connection."""
        commit_request = {
            'Operation': 'COMMIT',
            'Transaction_ID': transaction_id,
            'Chunk_ID': chunk_id
        }
        self.send_to_chunkserver(chunkserver_id, 'REQUEST', commit_request)

    def send_abort_to_chunkserver(self, chunkserver_id, transaction_id):
        """Send ABORT message to a specific chunkserver using persistent connection."""
        abort_request = {
            'Operation': 'ABORT',
            'Transaction_ID': transaction_id
        }
        self.send_to_chunkserver(chunkserver_id, 'REQUEST', abort_request)

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