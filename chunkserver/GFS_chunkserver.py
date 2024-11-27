import os
import sys
import GFS_chunk_metadata
import socket
import message
import time
import threading
import uuid
RETRY_CONNECT_TO_MASTER_TIME = 8
class ChunkServer:
    def __init__(self, port, directory):
        self.chunkserver_id = 0
        self.chunk_directory = GFS_chunk_metadata.Chunk_Directory()
        self.message_manager = message.Message_Manager()
        self.master_socket = None
        self.client_socket = None
        self.port = port
        self.directory = directory
        # Track ongoing append transactions
        self.append_transactions = {}
        self.is_connected = False
        
    def connect_to_master(self):
        # retry_time = 8
        while not self.is_connected:
            try:
                self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.master_socket.connect(('localhost', 5010))
                self.is_connected = True
                print("Connected to master server")
                return True
            except ConnectionRefusedError:
                print(f"Failed to connect to master server. Retrying in {RETRY_CONNECT_TO_MASTER_TIME} seconds...")
                time.sleep(RETRY_CONNECT_TO_MASTER_TIME)
            except Exception as e:
                print(f"Unexpected error connecting to master: {e}. Retrying in {RETRY_CONNECT_TO_MASTER_TIME} seconds...")
                time.sleep(RETRY_CONNECT_TO_MASTER_TIME)
                
    def reconnect_to_master(self):
        print("Attempting to reconnect to master server...")
        self.is_connected = False
        if self.master_socket:
            try:
                self.master_socket.close()
            except:
                pass
        return self.connect_to_master()


    def send_heartbeat(self):
        sleep_time = 5
        while True:
            # if not self.is_connected:
            #     time.sleep(sleep_time)
            #     continue
                
            try:
                self.message_manager.send_message(self.master_socket, 'HEARTBEAT', {'Operation': 'HEARTBEAT'})
            except:
                print(f"Error sending heartbeat. Attempting reconnection in {RETRY_CONNECT_TO_MASTER_TIME} seconds...")
                self.is_connected = False
                time.sleep(RETRY_CONNECT_TO_MASTER_TIME)
                if self.reconnect_to_master():
                    # Re-register with master after reconnection
                    try:
                        self.message_manager.send_message(self.master_socket, 'REGISTER',
                                                      {'Address':('localhost', self.port)})
                        response, data = self.message_manager.receive_message(self.master_socket)
                        if response == 'RESPONSE' and data['Status'] == 'SUCCESS':
                            self.chunkserver_id = data['Chunkserver_ID']
                            print(f"Re-registered with master as chunkserver {self.chunkserver_id}")
                            # Resend chunk directory
                            self.message_manager.send_message(self.master_socket, 'CHUNK_DIRECTORY',
                                                          {'Chunk_Directory': self.get_chunk_directory()})
                            print("Resent chunk directory to master")
                    except Exception as e:
                        print(f"Error re-registering with master: {e}")
                        self.is_connected = False
            finally:
                time.sleep(sleep_time)


    def handle_read(self, client_socket, request_data):
        file_name = request_data['File_Name']
        chunk_number = request_data['Chunk_Number']
        chunk_id = f'{file_name}_{chunk_number}'
        start = request_data['Start']
        end = request_data['End']
        chunk = self.chunk_directory.get_chunk(chunk_id)
        if chunk is None:
            response = {'Status': 'FAILED', 'Error': 'Chunk not found'}
        else:
            response = {'Status': 'SUCCESS', 'Data': chunk.read(start,end)}
        self.message_manager.send_message(client_socket, 'RESPONSE', response)

    def client_thread(self,client_socket):
        try:
                request_type, request_data = self.message_manager.receive_message(client_socket)
                if request_type == 'REQUEST':
                    if request_data['Operation'] == 'READ':
                        self.handle_read(client_socket, request_data)
                    
        except Exception as e:
            print(f"Error handling client request: {e}")
        finally:
            client_socket.close()

    def update_leases(self):
        while True:
            for chunk_id, lease in self.chunk_directory.lease_dict.items():
                self.chunk_directory.lease_dict[chunk_id] -= 1
                if lease == 5:
                    # Renew the lease
                    self.message_manager.send_message(self.master_socket, 'REQUEST',
                                                      {'Operation': 'RENEW_LEASE', 'Chunk_ID': chunk_id})
                    response, data = self.message_manager.receive_message(self.master_socket)
                    if response == 'RESPONSE':
                        if data['Status'] == 'SUCCESS':
                            self.chunk_directory.lease_dict[chunk_id] = 60  # Reset lease time
                        else:
                            # If lease renewal fails, remove the lease
                            del self.chunk_directory.lease_dict[chunk_id]
            time.sleep(1)  # Check leases every second

    def client_chunkserver_thread(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.bind(('localhost', self.port))
        print("Chunkserver listening for client requests on port", self.port)
        self.client_socket.listen(5)
        # Accept client connections
        while True:
            client_socket, address = self.client_socket.accept()
            # Start a new thread to handle the client request
            client_handler = threading.Thread(target=self.client_thread, args=(client_socket,))
            client_handler.start()


    # def handle_master_append_transaction(self, request_data):
    #     """Handle a master append transaction."""
    #     transaction_id = request_data['Transaction_ID']
    #     operation = request_data['Operation']
    #     chunk_id = f"{request_data['File_Name']}_{request_data['Chunk_Number']}"

    #     print(f"[DEBUG] Received {operation} operation for Transaction_ID {transaction_id}, Chunk_ID {chunk_id}")
    #     print(f"[DEBUG] Full request data: {request_data}")  # Add this line to see complete request data

    #     try:
    #         if operation == 'PREPARE':
    #             print(f"[DEBUG] Handling PREPARE operation for Transaction_ID {transaction_id}")
                
    #             # Debug chunk directory state
    #             print(f"[DEBUG] Current chunk directory state: {self.chunk_directory}")
    #             print(f"[DEBUG] Looking up chunk: {chunk_id}")
    #             # Verify chunk exists and is writable
    #             # try:
    #             #     chunk = self.chunk_directory.get_chunk(chunk_id)
    #             #     print(f"[DEBUG] Retrieved chunk: {chunk}")
    #             # except Exception as chunk_error:
    #             #     print(f"[DEBUG] Error retrieving chunk: {str(chunk_error)}")
    #             #     return {'Status': 'FAILED', 'Reason': f'Error accessing chunk: {str(chunk_error)}'}

    #             # if chunk is None:
    #             #     print(f"[DEBUG] PREPARE failed: Chunk {chunk_id} not found")
    #             #     return {'Status': 'FAILED', 'Reason': 'Chunk not found'}

    #             # # Validate chunk attributes
    #             # if not hasattr(chunk, 'append'):
    #             #     print(f"[DEBUG] PREPARE failed: Chunk {chunk_id} does not support append operation")
    #             #     return {'Status': 'FAILED', 'Reason': 'Chunk does not support append operation'}

    #             # Store transaction details
    #             try:
    #                 self.append_transactions[transaction_id] = {
    #                     'Chunk_ID': chunk_id,
    #                     'File_Name': request_data['File_Name'],
    #                     'Data': request_data['Data'],
    #                     'Status': 'PREPARED'
    #                 }
    #             except Exception as tx_error:
    #                 print(f"[DEBUG] Error storing transaction: {str(tx_error)}")
    #                 return {'Status': 'FAILED', 'Reason': f'Error storing transaction: {str(tx_error)}'}

    #             print(f"[DEBUG] Transaction {transaction_id} prepared successfully for Chunk_ID {chunk_id}")
    #             return {'Status': 'READY'}

    #         # Rest of the code remains the same...
    #         elif operation == 'COMMIT':
    #             print(f"[DEBUG] Handling COMMIT operation for Transaction_ID {transaction_id}")
                
    #             # Verify transaction exists
    #             if transaction_id not in self.append_transactions:
    #                 print(f"[DEBUG] COMMIT failed: Unknown Transaction_ID {transaction_id}")
    #                 return {'Status': 'FAILED', 'Reason': 'Unknown transaction'}

    #             transaction = self.append_transactions[transaction_id]
                
    #             try:
    #                 chunk = self.chunk_directory.get_chunk(transaction['Chunk_ID'])
    #             except Exception as chunk_error:
    #                 print(f"[DEBUG] Error retrieving chunk during COMMIT: {str(chunk_error)}")
    #                 return {'Status': 'FAILED', 'Reason': f'Error accessing chunk: {str(chunk_error)}'}

    #             if not chunk:
    #                 print(f"[DEBUG] COMMIT failed: Chunk {transaction['Chunk_ID']} not found")
    #                 return {'Status': 'FAILED', 'Reason': 'Chunk not found'}

    #             # Append data to chunk
    #             try:
    #                 chunk.append(transaction['Data'])
    #             except Exception as append_error:
    #                 print(f"[DEBUG] Error appending data: {str(append_error)}")
    #                 return {'Status': 'FAILED', 'Reason': f'Error appending data: {str(append_error)}'}

    #             print(f"[DEBUG] Data appended to Chunk_ID {transaction['Chunk_ID']} for Transaction_ID {transaction_id}")

    #             # Clean up transaction
    #             del self.append_transactions[transaction_id]
    #             print(f"[DEBUG] Transaction {transaction_id} committed and cleaned up successfully")
    #             return {'Status': 'SUCCESS'}

    #         elif operation == 'ABORT':
    #             print(f"[DEBUG] Handling ABORT operation for Transaction_ID {transaction_id}")
                
    #             # Remove transaction if it exists
    #             if transaction_id in self.append_transactions:
    #                 del self.append_transactions[transaction_id]
    #                 print(f"[DEBUG] Transaction {transaction_id} aborted successfully")
    #             else:
    #                 print(f"[DEBUG] No transaction found to abort for Transaction_ID {transaction_id}")
    #             return {'Status': 'ABORTED'}

    #     except Exception as e:
    #         print(f"[DEBUG] Exception occurred while handling {operation} for Transaction_ID {transaction_id}")
    #         print(f"[DEBUG] Exception type: {type(e)}")
    #         print(f"[DEBUG] Exception details: {str(e)}")
    #         print(f"[DEBUG] Stack trace:", exc_info=True)
    #         return {'Status': 'FAILED', 'Reason': str(e)}



    def handle_master_append_transaction(self, request_data):
        """
        Handle master append transactions with explicit sub-transaction tracking.
        
        Maintains a list of chunk_ids to be modified and supports partial commits/aborts.
        Provides chunk allocation guidance for primary server.
        """
        transaction_id = request_data['Transaction_ID']
        operation = request_data['Operation']
        file_name = request_data['File_Name']
        chunk_number = request_data['Chunk_Number']
        chunk_id = f"{file_name}_{chunk_number}"
        is_primary = request_data.get('Primary', False)

        print(f"[DEBUG] Received {operation} operation for Transaction_ID {transaction_id}, Chunk_ID {chunk_id}")

        try:
            # Initialize transaction if not exists
            if transaction_id not in self.append_transactions:
                self.append_transactions[transaction_id] = {
                    'pending_chunks': [],  # List of chunk_ids to be modified
                    'prepared_chunks': {},  # Detailed info about prepared chunks
                    'committed_chunks': [],  # Successfully committed chunks
                    'status': 'INITIALIZING'
                }

            transaction = self.append_transactions[transaction_id]

            if operation == 'PREPARE':
                data = request_data.get('Data', '')
                data_length = request_data.get('Data_Length', 0)

                # Check if this chunk is already being prepared
                if chunk_id in transaction['pending_chunks'] or chunk_id in transaction['committed_chunks']:
                    return {'Operation': 'PREPARE', 'Status': 'ALREADY_PREPARED','Transaction_ID': transaction_id}

                # If primary, check chunk allocation requirements
                if is_primary:
                    try:
                        # Get the last chunk for this file
                        last_chunk = self.chunk_directory.get_chunk(chunk_id)
                        if last_chunk:
                            remaining_space = last_chunk.get_remaining_space()
                            # print(f"[DEBUG] Last chunk {chunk_id} has {remaining_space} bytes remaining")

                            if remaining_space > 0:
                                if data_length <= remaining_space:
                                    # All data can fit in the current chunk
                                    return {
                                        'Operation': 'PREPARE',
                                        'Transaction_ID': transaction_id,
                                        'Status': 'NO_NEW_CHUNK_NEEDED',
                                        'Available_Space': remaining_space,
                                        'Can_Fit_All': True
                                    }
                                else:
                                    # Only partial data can fit
                                    return {
                                        'Operation': 'PREPARE',
                                        'Transaction_ID': transaction_id,
                                        'Status': 'PARTIAL_CHUNK_NEEDED',
                                        'Available_Space': remaining_space,
                                        'Can_Fit_All': False
                                    }
                    except Exception as chunk_error:
                        print(f"[Error] Error checking last chunk: {str(chunk_error)}")
                        return {'Status': 'FAILED', 'Reason': f'Error checking last chunk: {str(chunk_error)}','Transaction_ID': transaction_id,'Chunk_ID':chunk_id}
                else:
                    # Store chunk preparation details
                    transaction['pending_chunks'].append(chunk_id)
                    transaction['prepared_chunks'][chunk_id] = {
                        'Chunk_ID': chunk_id,
                        'File_Name': file_name,
                        'Data': data,
                        'Status': 'PREPARED',
                        'Is_Primary': is_primary
                    }
                    self.append_transactions[transaction_id] = transaction
                    return {'Operation': 'PREPARE', 'Status': 'READY','Transaction_ID': transaction_id,'Chunk_ID':chunk_id}

            elif operation == 'COMMIT':
                # Verify this specific chunk is pending
                if chunk_id not in transaction['pending_chunks']:
                    return {'Operation': 'COMMIT', 'Status': 'FAILED', 'Reason': 'Chunk not prepared','Transaction_ID': transaction_id}

                # Attempt to commit the specific chunk
                try:
                    chunk_info = transaction['prepared_chunks'][chunk_id]
                    chunk = self.chunk_directory.get_chunk(chunk_id)

                    if not chunk:
                        chunk = self.chunk_directory.add_chunk(
                            file_name,
                            chunk_number,
                            chunk_info['Data'],
                            chunk_info['Is_Primary']
                        )
                        if not chunk:
                            return {'Status': 'FAILED', 'Reason': f'Could not create chunk {chunk_id}','Transaction_ID': transaction_id,'Chunk_ID':chunk_id}
                    else:
                        self.chunk_directory.append_chunk(chunk_id, chunk_info['Data'])

                    # Move chunk from pending to committed
                    transaction['pending_chunks'].remove(chunk_id)
                    transaction['committed_chunks'].append(chunk_id)

                    return {'Operation': 'COMMIT', 'Status': 'SUCCESS', 'Chunk_ID': chunk_id,'Transaction_ID': transaction_id,'Chunk_ID':chunk_id}

                except Exception as commit_error:
                    print(f"[Error] Error committing chunk {chunk_id}: {str(commit_error)}")
                    return {'Status': 'FAILED', 'Reason': f'Error committing chunk {chunk_id}','Transaction_ID': transaction_id,'Chunk_ID':chunk_id}

            elif operation == 'ABORT':
                # Remove this specific chunk from pending
                if chunk_id in transaction['pending_chunks']:
                    transaction['pending_chunks'].remove(chunk_id)
                    
                    # Remove prepared chunk info
                    if chunk_id in transaction['prepared_chunks']:
                        del transaction['prepared_chunks'][chunk_id]

                # If no more pending chunks, clean up entire transaction
                if not transaction['pending_chunks']:
                    del self.append_transactions[transaction_id]

                return {'Operation': 'ABORT', 'Status': 'ABORTED', 'Chunk_ID': chunk_id,'Transaction_ID': transaction_id,'Chunk_ID':chunk_id}

        except Exception as e:
            print(f"[Error] Exception in transaction {transaction_id}: {str(e)}")
            return {'Status': 'FAILED', 'Reason': str(e),'Transaction_ID': transaction_id,'Chunk_ID':chunk_id}


    def handle_master_commands(self):
        """Continuously handle commands from the master server."""
        while True:
            try:
                # Receive message from master
                request_type, request_data = self.message_manager.receive_message(self.master_socket)

                if request_type == 'REQUEST':
                    operation = request_data.get('Operation')
                    print(f"[DEBUG] Handling REQUEST operation: {operation}")

                    if operation in ['PREPARE', 'COMMIT', 'ABORT']:
                        # Handle append transaction commands
                        response = self.handle_master_append_transaction(request_data)
                        # print(f"[DEBUG] Response for {operation} operation: {response}")
                        self.message_manager.send_message(self.master_socket, 'RESPONSE', response)

                    elif operation == 'CREATE':
                        # Handle CREATE operation
                        file_name = request_data['File_Name']
                        chunk_number = request_data['Chunk_Number']
                        is_primary = request_data['Primary']
                        data = request_data['Data']
                        print(chunk_number)

                        # Create new chunk
                        if self.chunk_directory.add_chunk(file_name, chunk_number, data, is_primary):
                            response = {'Status': 'SUCCESS'}
                        else:
                            response = {'Status': 'FAILED', 'Error': 'Could not create chunk'}
                            print(f"[DEBUG] Failed to create chunk {chunk_number} for file '{file_name}'.")
                        self.message_manager.send_message(self.master_socket, 'RESPONSE', response)

                    elif operation == 'DELETE':
                        # Handle DELETE operation
                        print("[DEBUG] Handling DELETE operation.")
                        file_name = request_data['File_Name']
                        chunk_number = request_data['Chunk_Number']
                        base_chunk_id = f"{file_name}_{chunk_number}"
                        
                        # try:
                        # Find all files matching the pattern file_name_chunk_number_*.chunk
                        matching_files = []
                        for file in os.listdir():
                            # Split the filename into components
                            if not file.endswith('.chunk'):
                                continue
                            
                            file_base = file.rsplit('.', 1)[0]  # Remove .chunk extension
                            file_components = file_base.split('_')
                            
                            # Check if this file matches our pattern
                            if len(file_components) >= 2:
                                file_prefix = '_'.join(file_components[:-1])  # Everything except version
                                if file_prefix == base_chunk_id:
                                    matching_files.append(file)
                        
                        if not matching_files:
                            print(f"[Error] No files found matching pattern {base_chunk_id}_*.chunk")
                            response = {'Status': 'FAILED', 'Error': 'No matching chunks found'}
                        else:
                            # Delete all matching files
                            deletion_success = True
                            failed_files = []
                            
                            for file in matching_files:
                                try:
                                    os.remove(file)
                                    print(f"[DEBUG] Successfully deleted file: {file}")
                                    
                                    # Remove from chunk directory if present
                                    chunk_id = file.rsplit('.', 1)[0]  # Remove .chunk extension
                                    if chunk_id in self.chunk_directory.chunk_dict:
                                        self.chunk_directory.delete_chunk(chunk_id)
                                        # print(f"[DEBUG] Removed {chunk_id} from chunk directory")
                                except OSError as e:
                                    print(f"[Error] Failed to delete file {file}: {str(e)}")
                                    deletion_success = False
                                    failed_files.append(file)
                            
                            if deletion_success:
                                response = {'Status': 'SUCCESS'}
                            else:
                                response = {
                                    'Status': 'PARTIAL',
                                    'Error': f'Failed to delete some files: {", ".join(failed_files)}'
                                }
                            
                            print(f"[DEBUG] Deleted {len(matching_files) - len(failed_files)} out of {len(matching_files)} matching files")
                        
                        # except Exception as e:
                        #     print(f"[DEBUG] Error during DELETE operation: {str(e)}")
                        #     response = {'Status': 'FAILED', 'Error': f'Delete operation failed: {str(e)}'}
                        
                        self.message_manager.send_message(self.master_socket, 'RESPONSE', response)


                elif request_type == 'HEARTBEAT_ACK':
                    pass
                    # Handle heartbeat acknowledgment
                    # print("[DEBUG] Received HEARTBEAT_ACK from master.")

                else:
                    # Handle unknown request type
                    print(f"[DEBUG] Unknown request type received: {request_type}. Data: {request_data}")
                    break

            except Exception as e:
                # Log exceptions and exit loop
                print(f"[Error] Error handling master command: {e}")
                break

    
    
    def load_chunk_directory(self):
        # Create a directory for the chunkserver
        os.makedirs(self.directory, exist_ok=True)
        # Jump into the chunkserver directory
        os.chdir(self.directory)
        # Load the chunk directory from disk by looking for chunk files
        for file in os.listdir():
            if file.endswith('.chunk'):
                # Load the chunk file
                print(f"Loading chunk file: {file.split('.')[0]}")
                file_name, chunk_number, version = file.split('.')[0].split('_')
                with open(file, 'r') as f:
                    data = f.read()
                # Add the chunk to the chunk directory
                self.chunk_directory.load_chunk(file_name,int(chunk_number),int(version),data)
        os.chdir('..')
    
    # Get JSON serializable chunk directory
    def get_chunk_directory(self):
        chunk_dir={}
        for chunk_id, chunk_data in self.chunk_directory.chunk_dict.items():
            # Remove the chunk object
            file_name,chunk_number=chunk_data['Chunk'].get_chunk_info().split(',')
            chunk_data['File_Name']=file_name
            chunk_data['Chunk_Number']=int(chunk_number)
            chunk_dir[chunk_id] = chunk_data.copy()
            chunk_dir[chunk_id].pop('Chunk')
        return chunk_dir
    
    def start_chunkserver(self):
        # Initial connection to master with retry
        if not self.connect_to_master():
            print("Failed to establish initial connection to master")
            return

        # Register with the master
        self.message_manager.send_message(self.master_socket, 'REGISTER',
                                          {'Address':('localhost', self.port)})

        # Receive the master's response
        response, data = self.message_manager.receive_message(self.master_socket)

        # Check if the master accepted the registration
        if response == 'RESPONSE' and data['Status'] == 'SUCCESS':
            self.chunkserver_id = data['Chunkserver_ID']
            print(f"Registered with master as chunkserver {self.chunkserver_id}")
            
            # Load the chunk directory from disk
            self.load_chunk_directory()
            os.chdir(self.directory)
            # Send the directory to the master
            self.message_manager.send_message(self.master_socket, 'CHUNK_DIRECTORY',
                                              {'Chunk_Directory': self.get_chunk_directory()})
            print("Sent chunk directory to master")           
            
            # Create threads for various operations
            heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            lease_thread = threading.Thread(target=self.update_leases)
            client_server_thread = threading.Thread(target=self.client_chunkserver_thread)
            master_command_thread = threading.Thread(target=self.handle_master_commands)

            # Start all threads
            heartbeat_thread.daemon = True
            lease_thread.daemon = True
            client_server_thread.daemon = True
            master_command_thread.daemon = True

            heartbeat_thread.start()
            lease_thread.start()
            client_server_thread.start()
            master_command_thread.start()

            print(f"Chunkserver {self.chunkserver_id} started successfully")
            # Keep the main thread alive
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nShutting down chunkserver...")
        else:
            print("Registration with master failed")
            self.master_socket.close()
            return

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 GFS_chunkserver.py port directory")
        sys.exit(1)
    port=int(sys.argv[1])
    directory=sys.argv[2]
    chunkserver = ChunkServer(port, directory)
    chunkserver.start_chunkserver()
