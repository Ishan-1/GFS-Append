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
        self.file_chunks = {}  # key: file_name, value: list of Chunk_Numbers
        self.replicas = NO_OF_REPLICAS
        self.message_manager = message.Message_Manager()
        self.lock = threading.Lock()
        self.append_transactions = {}
        self.chunkserver_metadata = {}  # New attribute added here for storing metadata of failed chunkservers

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
        """Extended registration to handle potential chunkserver recovery."""
        with self.lock: 
            chunkserver_id = len(self.chunkservers) + 1
            chunkserver_address = tuple(data.get('Address'))
            
            # Check if this is a known chunkserver trying to recover
            recovered_id = None
            for stored_id, metadata in self.chunkserver_metadata.items():
                if metadata['address'] == chunkserver_address:
                    recovered_id = stored_id
                    chunkserver_id = recovered_id
                    break
            
            # Create message queue for this chunkserver
            message_queue = queue.Queue()
            
            self.chunkservers[chunkserver_id] = {
                'connection': conn,
                'last_heartbeat': time.time(),
                'address': chunkserver_address,
                'message_queue': message_queue
            }
        
        print(f"{'Recovered' if recovered_id else 'Registered'} chunkserver {chunkserver_id} at {chunkserver_address}")
        
        # Send acknowledgment with chunkserver ID
        response = {
            'Status': 'SUCCESS', 
            'Chunkserver_ID': chunkserver_id,
            'Is_Recovery': bool(recovered_id)
        }
        self.message_manager.send_message(conn, 'RESPONSE', response)
        
        # If recovering, restore previous metadata
        if recovered_id:
            self.recover_chunkserver_metadata(chunkserver_id)
        
        # Start dedicated threads for this chunkserver
        threading.Thread(target=self.handle_chunkserver_messages, 
                       args=(chunkserver_id, conn), 
                       daemon=True).start()
        threading.Thread(target=self.process_chunkserver_queue, 
                       args=(chunkserver_id,), 
                       daemon=True).start()

        return chunkserver_id


    def handle_chunkserver_messages(self, chunkserver_id, conn):
        """Continuously handle messages from a specific chunkserver."""
        while True:
            try:
                request_type, request_data = self.message_manager.receive_message(conn)
                if request_type == 'HEARTBEAT':
                    # print("Heartbeat")
                    self.handle_heartbeat(chunkserver_id)
                elif request_type == 'CHUNK_DIRECTORY':
                    self.update_chunk_directory(request_data,chunkserver_id)
                elif request_type == 'RESPONSE':
                    self.handle_chunkserver_response(chunkserver_id, request_data)
                else:
                    print(f"Unknown message type from chunkserver {chunkserver_id}: {request_type}")
                    self.handle_chunkserver_failure(chunkserver_id)

                    # handle_chunkserver_failure
                
            except Exception as e:
                print(f"Error handling messages from chunkserver {chunkserver_id}: {e}")
                self.handle_chunkserver_failure(chunkserver_id)
                break

    def handle_chunkserver_response(self, chunkserver_id, response_data):
        """Handle responses from chunkservers for operations."""

        transaction_id = response_data.get('Transaction_ID')
        operation = response_data.get('Operation')
        status = response_data.get('Status')
        if transaction_id and transaction_id in self.append_transactions:
            transaction = self.append_transactions[transaction_id]
            if operation == 'PREPARE':
                transaction.setdefault('Prepare_Responses', []).append(response_data)
            elif operation == 'COMMIT':
                transaction.setdefault('Commit_Responses', []).append(response_data)
            elif operation == 'ABORT':
                transaction['Status'] = 'ABORTED'
            self.append_transactions[transaction_id] = transaction
        else:
            print(f"Status for operation {operation}:{status} from chunkserver {chunkserver_id}")

    def process_chunkserver_queue(self, chunkserver_id):
        """Process messages in the chunkserver's queue."""
        while True:
            try:
                # with self.lock:
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
        # print(f"[DEBUG] Queuing message for chunkserver {chunkserver_id}: Type={message_type}, Data={message_data}")
        # print(self.chunkservers)
        if chunkserver_id in self.chunkservers:
            self.chunkservers[chunkserver_id]['message_queue'].put({
                'type': message_type,
                'data': message_data
            })
            # print(f"[DEBUG] Message queued for chunkserver {chunkserver_id}.")
        else:
            print(f"[Error] Chunkserver {chunkserver_id} not found. Cannot queue message.")


    def handle_heartbeat(self, chunkserver_id):
        """Update heartbeat timestamp for a chunkserver."""
        if chunkserver_id in self.chunkservers:
                self.chunkservers[chunkserver_id]['last_heartbeat'] = time.time()
                self.send_to_chunkserver(chunkserver_id, 'HEARTBEAT_ACK', {'Status': 'OK'})
                # print("Heartbeat from chunkserver",chunkserver_id)

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
        """Handle chunkserver failure by cleaning up its resources and storing metadata."""
        with self.lock:
            if chunkserver_id in self.chunkservers:
                    # Store metadata before deletion
                self.store_chunkserver_metadata(chunkserver_id)
                
                # Close connection
                try:
                    self.chunkservers[chunkserver_id]['connection'].close()
                except:
                    pass
            
                # Remove from active chunkservers
                del self.chunkservers[chunkserver_id]
                
                # Remove chunks associated with this chunkserver
                for Chunk_Number, servers in list(self.chunk_locations.items()):
                    if chunkserver_id in servers:
                        servers.remove(chunkserver_id)
                        if not servers:
                            del self.chunk_locations[Chunk_Number]
                
                print(f"Chunkserver {chunkserver_id} removed due to failure. Metadata stored for recovery.")

    def store_chunkserver_metadata(self, chunkserver_id):
        """Store metadata for a failed chunkserver to aid in recovery."""
        if chunkserver_id in self.chunkservers:
            # Store the original address and chunks hosted on this chunkserver
            chunkserver_info = self.chunkservers[chunkserver_id]
            
            # Collect chunks hosted on this chunkserver
            hosted_chunks = []
            for chunk_id, server_list in self.chunk_locations.items():
                if chunkserver_id in server_list:
                    hosted_chunks.append(chunk_id)
            
            # Store comprehensive metadata
            self.chunkserver_metadata[chunkserver_id] = {
                'address': chunkserver_info['address'],
                'hosted_chunks': hosted_chunks,
                'last_known_timestamp': time.time()
            }
    def recover_chunkserver_metadata(self, chunkserver_id):
        """Restore metadata for a recovered chunkserver."""
        with self.lock:
            if chunkserver_id in self.chunkserver_metadata:
                metadata = self.chunkserver_metadata[chunkserver_id]
                
                # Restore chunks to chunk_locations
                for chunk_id in metadata['hosted_chunks']:
                    if chunk_id in self.chunk_locations:
                        if chunkserver_id not in self.chunk_locations[chunk_id]:
                            self.chunk_locations[chunk_id].append(chunkserver_id)
                    else:
                        self.chunk_locations[chunk_id] = [chunkserver_id]
                
                # Derive file_chunks from restored chunk information
                for chunk_id in metadata['hosted_chunks']:
                    # Assuming chunk_id format is 'filename_chunkNumber'
                    file_name = chunk_id.rsplit('_', 1)[0]
                    if file_name not in self.file_chunks:
                        self.file_chunks[file_name] = [chunk_id]
                    elif chunk_id not in self.file_chunks[file_name]:
                        self.file_chunks[file_name].append(chunk_id)
                
                print(f"Recovered metadata for Chunkserver {chunkserver_id}")
                
                # Optionally remove from stored metadata after recovery
                del self.chunkserver_metadata[chunkserver_id]
                return metadata['address']
            
            return None

                
    def update_chunk_directory(self, data, chunkserver_id):
        """Update chunk directory from chunkserver."""
        chunk_directory = data.get('Chunk_Directory', {})
        
        with self.lock:
            for chunk_id, chunk_info in chunk_directory.items():
                # Parse chunk info
                version = chunk_info.get('Version', 1)
                primary = chunk_info.get('Primary', False)
                
                # Extract file_name from chunk_id
                file_name = chunk_id.rsplit('_', 1)[0]
                chunk_number = chunk_id.rsplit('_', 1)[1]
                
                # Generate new chunk_id with next available chunk number
                new_chunk_id = self.generate_chunk_id(file_name,chunk_number)
                
                # Update file_chunks
                if file_name not in self.file_chunks:
                    self.file_chunks[file_name] = [new_chunk_id]
                else:
                    self.file_chunks[file_name].append(new_chunk_id)
                
                # Update chunk_locations
                if new_chunk_id not in self.chunk_locations:
                    self.chunk_locations[new_chunk_id] = [chunkserver_id]
                elif chunkserver_id not in self.chunk_locations[new_chunk_id]:
                    self.chunk_locations[new_chunk_id].append(chunkserver_id)
                
                # print(f"[DEBUG] Updated chunk directory: file={file_name}, chunk_id={new_chunk_id}, server={chunkserver_id}")
                

    def handle_client_request(self, conn, data):
        """Handle client operations: CREATE, DELETE, READ, APPEND."""
        operation = data.get('Operation')
        file_name = data.get('File_Name')
        print("Clent connected , operation:" , operation)
        if operation == 'CREATE':
            data_to_write = data.get('Data')
            self.handle_create(conn, file_name, data_to_write)
        elif operation == 'DELETE':
            self.handle_delete(conn, file_name)
        elif operation == 'READ':
            start_byte = data.get('Start_Byte', 0)
            end_byte = data.get('End_Byte', sys.maxsize)
            self.handle_read(conn, file_name, start_byte, end_byte)
        elif operation == 'APPEND':
            data_length = data.get('Data_Length', 0)
            data_to_append = data.get('Data')
            self.handle_append(conn, file_name, data_length,data_to_append)
        else:
            response = {'Status': 'FAILED', 'Error': 'Unknown operation'}
            self.message_manager.send_message(conn, 'RESPONSE', response)



    def generate_chunk_id(self, file_name, chunk_number):
        """Generate a unique chunk ID combining file name and chunk number."""
        return f"{file_name}_{chunk_number}"

    def parse_chunk_id(self, chunk_id):
        """Parse chunk ID to get file name and chunk number."""
        file_name, chunk_number = chunk_id.rsplit('_', 1)
        return file_name, int(chunk_number)

    def handle_create(self, conn, file_name, data):
        """Handle CREATE operation directly with chunkservers."""
        data_length = len(data)
        num_chunks = max(1, math.ceil(data_length / CHUNK_SIZE))
        
        chunks = []
        file_chunk_ids = []
        next_chunk_number = 1
        for i in range(num_chunks):
            chunk_number = next_chunk_number
            next_chunk_number += 1
            chunk_id = self.generate_chunk_id(file_name, chunk_number)

            selected_servers = self.select_chunkservers()
            if not selected_servers:
                response = {'Status': 'FAILED', 'Error': 'Insufficient chunkservers available'}
                print(f"ERROR: No chunkservers available for Chunk ID: {chunk_id}")
                self.message_manager.send_message(conn, 'RESPONSE', response)
                return

            primary_server = selected_servers[0]
            chunk_data = data[i * CHUNK_SIZE: (i + 1) * CHUNK_SIZE]
            self.chunk_locations[chunk_id] = selected_servers
            chunks.append({
                'Chunk_ID': chunk_id,
                'Primary_Server': primary_server,
                'Servers': selected_servers,
                'Data': chunk_data
            })
            file_chunk_ids.append(chunk_id)

        self.file_chunks[file_name] = file_chunk_ids

        # Send chunks to chunkservers
        success = True
        for chunk in chunks:
            chunk_success = True
            primary_server = chunk['Primary_Server']
            servers = chunk['Servers']

            for cs_id in servers:
                is_primary = (cs_id == primary_server)
                try:
                    chunk_number = self.parse_chunk_id(chunk['Chunk_ID'])[1]
                    create_req = {
                        'Operation': 'CREATE',
                        'Chunk_Number': chunk_number,
                        'File_Name': file_name,
                        'Data': chunk['Data'],
                        'Primary': is_primary
                    }
                    self.send_to_chunkserver(cs_id, 'REQUEST', create_req)
                except Exception as e:
                    print(f"ERROR: Failed to send Chunk ID {chunk['Chunk_ID']} to server {cs_id}: {e}")
                    if is_primary:
                        chunk_success = False

            if not chunk_success:
                print(f"ERROR: Failed to successfully send Chunk ID {chunk['Chunk_ID']} to its primary server.")
                success = False
                break

        response = {
            'Status': 'SUCCESS' if success else 'FAILED',
            'Message': f"File '{file_name}' {'created successfully' if success else 'creation failed'}"
        }
        self.message_manager.send_message(conn, 'RESPONSE', response)

    
    def handle_delete(self, conn, file_name):
        """Handle DELETE operation directly with chunkservers."""
        # print(f"[DEBUG] Received DELETE request for file: {file_name}")
        
        chunk_ids = self.file_chunks.get(file_name, [])
        if not chunk_ids:
            # print(f"[DEBUG] File '{file_name}' not found in file_chunks.")
            response = {'Status': 'FAILED', 'Error': 'File not found'}
            self.message_manager.send_message(conn, 'RESPONSE', response)
            return

        # print(f"[DEBUG] Chunks associated with '{file_name}': {chunk_ids}")
        success = True
        chunk_ids = list(set(chunk_ids))
        for chunk_id in chunk_ids:
            servers = self.chunk_locations.get(chunk_id, [])
            # print(f"[DEBUG] Processing chunk '{chunk_id}' located on servers: {servers}")
            
            chunk_success = False
            for cs_id in servers:
                try:
                    chunk_number = self.parse_chunk_id(chunk_id)[1]
                    delete_req = {
                        'Operation': 'DELETE',
                        'Chunk_Number': chunk_number,
                        'File_Name': file_name
                    }
                    # print(f"[DEBUG] Sending DELETE request for chunk '{chunk_id}' to chunkserver '{cs_id}': {delete_req}")
                    self.send_to_chunkserver(cs_id, 'REQUEST', delete_req)
                    chunk_success = True
                    # print(f"[DEBUG] Successfully deleted chunk '{chunk_id}' from chunkserver '{cs_id}'")
                except Exception as e:
                    print(f"[ERROR] Error deleting chunk '{chunk_id}' from chunkserver '{cs_id}': {e}")
            
            if chunk_success:
                # print(f"[DEBUG] Chunk '{chunk_id}' successfully deleted from all chunkservers.")
                del self.chunk_locations[chunk_id]
            else:
                print(f"[WARNING] Failed to delete chunk '{chunk_id}' from all chunkservers.")
                success = False

        if success:
            # print(f"[DEBUG] Successfully deleted all chunks for file '{file_name}'. Cleaning up metadata.")
            del self.file_chunks[file_name]
        else:
            print(f"[WARNING] Failed to delete one or more chunks for file '{file_name}'.")

        response = {
            'Status': 'SUCCESS' if success else 'FAILED',
            'Message': f"File '{file_name}' {'deleted successfully' if success else 'deletion failed'}"
        }
        # print(f"[DEBUG] Sending response to client: {response}")
        self.message_manager.send_message(conn, 'RESPONSE', response)

    
    
    
    
    

    def handle_read(self, conn, file_name, start_byte, end_byte):
        """Handle READ operation."""
        with self.lock:
            chunk_ids = self.file_chunks.get(file_name, [])
            # chunk_ids = list(set(chunk_ids))
            chunk_ids = sorted(set(chunk_ids))
            chunk_ids = sorted(chunk_ids, key=lambda x: int(x.split('_')[1]))

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
            # print("[DEBUG] chunk_ids: " , chunk_ids)
            # print("[DEBUG] relevant_chunks: " , relevant_chunks)
            
            chunks_info = []
            for chunk_id in relevant_chunks:
                Chunk_Number = self.parse_chunk_id(chunk_id)[1]
                chunks_info.append({
                    'Chunk_Number': Chunk_Number,
                    'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in self.chunk_locations[chunk_id]]
                })
        print(chunks_info)
        response = {'Status': 'SUCCESS', 'Chunks': chunks_info}
        self.message_manager.send_message(conn, 'RESPONSE', response)



    def handle_append(self, conn, file_name, data_length, data_to_append):
        """Handle APPEND operation by coordinating with chunkservers."""
        transaction_id = str(uuid.uuid4())
        self.append_transactions[transaction_id] = {'Prepare_Responses': [], 'Status': 'PENDING'}
        print(f"[DEBUG] Starting APPEND for file: {file_name}, transaction: {transaction_id}")

        chunks_to_write = []
        remaining_data = data_length
        next_chunk_number = 0
        # Step 1: Check last chunk for space (if file exists)
        if file_name in self.file_chunks and self.file_chunks[file_name]:
            last_chunk_id = self.file_chunks[file_name][-1]
            primary_server_id = self.chunk_locations[last_chunk_id][0]
            _, last_chunk_number = self.parse_chunk_id(last_chunk_id)
            next_chunk_number = last_chunk_number

            # print(f"[DEBUG] Last chunk found: {last_chunk_id}, primary server: {primary_server_id}")

            # Send PREPARE to primary server
            self.send_to_chunkserver(primary_server_id, 'REQUEST', {
                'Operation': 'PREPARE',
                'Transaction_ID': transaction_id,
                'File_Name': file_name,
                'Chunk_Number': last_chunk_number,
                'Data_Length': data_length,
                'Primary': True,
                'Data': data_to_append
            })

            # print(f"[DEBUG] PREPARE request sent to primary server: {primary_server_id} for chunk: {last_chunk_id}")

            # Wait for response from primary chunkserver
            start_time = time.time()
            while True:
                with self.lock:
                    prepare_responses = self.append_transactions[transaction_id].get('Prepare_Responses', [])
                    if prepare_responses:
                        response = prepare_responses[0]  # Primary response
                        # Pop the response from the list
                        self.append_transactions[transaction_id]['Prepare_Responses'] = []
                        # print(f"[DEBUG] Received response from primary: {response}")
                        break

                if time.time() - start_time > 10:  # Timeout
                    print(f"[DEBUG] PREPARE timed out for transaction: {transaction_id}")
                    self.send_to_chunkserver(primary_server_id,'REQUEST',{
                      'Operation': 'ABORT',
                      'Transaction_ID': transaction_id,
                      'File_Name': file_name,
                      'Chunk_Number': last_chunk_number,})
                    self.message_manager.send_message(conn, 'RESPONSE', {'Status': 'FAILED', 'Error': 'Timeout on PREPARE'})
                    return
                time.sleep(2)

            if response.get('Status') == 'NO_NEW_CHUNK_NEEDED':
                available_space = response.get('Available_Space')
                chunks_to_write.append({
                    'Chunk_ID': last_chunk_id,
                    'Chunkserver_IDs': self.chunk_locations[last_chunk_id],
                    'Data_Length': data_length,
                    'Data': data_to_append[:available_space]
                })
                remaining_data -= available_space
                # print(f"[DEBUG] Appending data to existing chunk: {last_chunk_id}, remaining data: {remaining_data}")
            elif response.get('Status') == 'PARTIAL_CHUNK_NEEDED':
                available_space = response.get('Available_Space')
                chunks_to_write.append({
                    'Chunk_ID': last_chunk_id,
                    'Chunkserver_IDs': self.chunk_locations[last_chunk_id],
                    'Data_Length': available_space,
                    'Data': data_to_append[:available_space]
                })
                remaining_data-=available_space
                next_chunk_number += 1
                # print(f"[DEBUG] Partial space used in chunk: {last_chunk_id}, remaining data: {remaining_data}")
        else:
            self.message_manager.send_message(conn, 'RESPONSE', {'Status': 'FAILED', 'Error': 'File not found'})
            return 
        # Step 2: Allocate new chunks for remaining data
        while remaining_data > 0:
            chunk_data_length = min(remaining_data, CHUNK_SIZE)
            selected_servers = self.select_chunkservers()
            if not selected_servers:
                # print(f"[DEBUG] Insufficient chunkservers for new chunk allocation.")
                self.send_to_chunkserver(primary_server_id,transaction_id,{
                  'Operation': 'ABORT',
                  'Transaction_ID': transaction_id,
                  'File_Name': file_name,
                  'Chunk_Number': last_chunk_number,
                  })
                self.message_manager.send_message(conn, 'RESPONSE', {
                    'Status': 'FAILED', 'Error': 'Insufficient chunkservers'
                })
                return

            chunk_id = self.generate_chunk_id(file_name, next_chunk_number)
            chunk_data = data_to_append[data_length - remaining_data:data_length - remaining_data + chunk_data_length]
            print(chunk_data)
            chunks_to_write.append({
                'Chunk_ID': chunk_id,
                'Chunkserver_IDs': selected_servers,
                'Data_Length': chunk_data_length,
                'Data': chunk_data
            })
            self.chunk_locations[chunk_id] = selected_servers
            remaining_data -= chunk_data_length
            next_chunk_number += 1

            # print(f"[DEBUG] New chunk allocated: {chunk_id}, servers: {selected_servers}, remaining data: {remaining_data}")
        
        # Step 3: Send PREPARE to all chunkservers for new chunks
        for chunk in chunks_to_write:
            for cs_id in chunk['Chunkserver_IDs']:
                is_primary = cs_id == chunk['Chunkserver_IDs'][0]
                file_name,chunk_number=self.parse_chunk_id(chunk['Chunk_ID'])
                print("Handling send for ",chunk['Chunk_ID'])
                print("Chunk Data: ",chunk['Data'])
                self.send_to_chunkserver(cs_id, 'REQUEST', {
                    'Operation': 'PREPARE',
                    'Transaction_ID': transaction_id,
                    'Chunk_Number': chunk_number,
                    'Data_Length': chunk['Data_Length'],
                    'File_Name': file_name,
                    'Primary': False,
                    'Data': chunk['Data']
                })
                time.sleep(2)
                print("Done")
            # print(f"[DEBUG] PREPARE sent for chunk: {chunk['Chunk_ID']}")

        # Step 4: Wait for all PREPARE responses
        start_time = time.time()
        while True:
            with self.lock:
                prepare_responses = self.append_transactions[transaction_id].get('Prepare_Responses', [])
                # print(f"[DEBUG] Response of Prepare: {prepare_responses}");
                if len(prepare_responses) >=(NO_OF_REPLICAS)*len(chunks_to_write):
                    print(f"[DEBUG] Received all PREPARE responses for transaction: {transaction_id}")
                    break

            if time.time() - start_time > 10:  # Timeout
                print(f"[DEBUG] PREPARE responses timed out for transaction: {transaction_id}")
                self.abort_append_transaction(transaction_id)
                self.message_manager.send_message(conn, 'RESPONSE', {'Status': 'FAILED', 'Error': 'Timeout on PREPARE'})
                return
            time.sleep(0.1)

        # Step 5: Send COMMIT to finalize
        for chunk in chunks_to_write:
            for cs_id in chunk['Chunkserver_IDs']:
                file_name,chunk_number=self.parse_chunk_id(chunk['Chunk_ID'])
                self.send_to_chunkserver(cs_id, 'REQUEST', {
                    'Operation': 'COMMIT',
                    'Transaction_ID': transaction_id,
                    'Chunk_Number': chunk_number,
                    'File_Name': file_name
                })
                time.sleep(0.1)
            # print(f"[DEBUG] COMMIT sent for chunk: {chunk['Chunk_ID']}")
        with self.lock:
            if file_name not in self.file_chunks:
                self.file_chunks[file_name] = []
            self.file_chunks[file_name].extend([chunk['Chunk_ID'] for chunk in chunks_to_write])

        # Step 6: Respond to client
        print(f"[DEBUG] APPEND operation successful for transaction: {transaction_id}")
        self.message_manager.send_message(conn, 'RESPONSE',{'Status': 'SUCCESS', 'Transaction_ID': transaction_id})




    def abort_append_transaction(self, transaction_id):
        with self.lock:
            transaction = self.append_transactions.get(transaction_id)

            if not transaction:
                print(f"[Error] No active transaction found for Transaction_ID {transaction_id}.")
                return

            # Extract chunk information
            prepare_responses = transaction.get('Prepare_Responses', [])
            chunks_to_abort = {response['Chunk_ID'] for response in prepare_responses}

            print(f"[DEBUG] Aborting transaction {transaction_id} for chunks: {chunks_to_abort}")

            # Remove uncommitted chunks
            for chunk_id in chunks_to_abort:
                chunkserver_ids = self.chunk_locations.get(chunk_id, [])
                file_name, chunk_number = self.parse_chunk_id(chunk_id)
                if file_name in self.file_chunks and chunk_id in self.file_chunks[file_name]:
                    self.file_chunks[file_name].remove(chunk_id)
                    # print(f"[DEBUG] Removed chunk {chunk_id} from file_chunks for file: {file_name}")
                if chunk_id in self.chunk_locations:
                    del self.chunk_locations[chunk_id]
                    # print(f"[DEBUG] Removed chunk {chunk_id} from chunk_locations.")

                # Notify chunkservers about abort
                for cs_id in chunkserver_ids:
                    self.send_to_chunkserver(cs_id, 'REQUEST', {
                        'Operation': 'ABORT',
                        'Transaction_ID': transaction_id,
                        'Chunk_Number': chunk_number,
                        'File_Name': file_name
                    })

            # Clean up transaction state
            del self.append_transactions[transaction_id]
            print(f"[DEBUG] Transaction {transaction_id} aborted and cleaned up.")


    
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
    
# Entry point for the master server application
if __name__ == "__main__":
    master_server = MasterServer()
    master_server.start()