import os
import sys
import GFS_chunk_metadata
import socket
import message
import time
import threading
import uuid

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
        retry_time = 8
        while not self.is_connected:
            try:
                self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.master_socket.connect(('localhost', 5010))
                self.is_connected = True
                print("Connected to master server")
                return True
            except ConnectionRefusedError:
                print(f"Failed to connect to master server. Retrying in {retry_time} seconds...")
                time.sleep(retry_time)
            except Exception as e:
                print(f"Unexpected error connecting to master: {e}. Retrying in {retry_time} seconds...")
                time.sleep(retry_time)
                
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
                print("Sending heartbeat")
            except:
                print("Error sending heartbeat. Attempting reconnection in 20 seconds...")
                self.is_connected = False
                time.sleep(20)
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
                                                          {'Chunk_Directory': self.chunk_directory.chunk_dict})
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
            for chunk_id, lease in list(self.chunk_directory.lease_dict.items()):
                self.chunk_directory.lease_dict[chunk_id]['Time'] -= 1
                if lease['Time'] == 5:
                    # Renew the lease
                    self.message_manager.send_message(self.master_socket, 'REQUEST',
                                                      {'Operation': 'RENEW_LEASE', 'Chunk_ID': chunk_id})
                    response, data = self.message_manager.receive_message(self.master_socket)
                    if response == 'RESPONSE':
                        if data['Status'] == 'SUCCESS':
                            self.chunk_directory.lease_dict[chunk_id]['Time'] = 60  # Reset lease time
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
    def handle_master_append_transaction(self, request_data):
        transaction_id = request_data['Transaction_ID']
        operation = request_data['Operation']
        chunk_id = f"{request_data['File_Name']}_{request_data['Chunk_Number']}"
        try:
            if operation == 'PREPARE':
                # Verify chunk exists and is writable
                chunk = self.chunk_directory.get_chunk(chunk_id)
                if chunk is None:
                    return {'Status': 'FAILED', 'Reason': 'Chunk not found'}

                # Store transaction details
                self.append_transactions[transaction_id] = {
                    'Chunk_ID': chunk_id,
                    'Data': request_data['Data'],
                    'Status': 'PREPARED'
                }
                return {'Status': 'READY'}

            elif operation == 'COMMIT':
                # Verify transaction exists
                if transaction_id not in self.append_transactions:
                    return {'Status': 'FAILED', 'Reason': 'Unknown transaction'}

                transaction = self.append_transactions[transaction_id]
                chunk = self.chunk_directory.get_chunk(transaction['Chunk_ID'])
                chunk.append(transaction['Data'])
                
                # Clean up transaction
                del self.append_transactions[transaction_id]
                return {'Status': 'SUCCESS'}

            elif operation == 'ABORT':
                # Remove transaction if it exists
                if transaction_id in self.append_transactions:
                    del self.append_transactions[transaction_id]
                return {'Status': 'ABORTED'}

        except Exception as e:
            return {'Status': 'FAILED', 'Reason': str(e)}

    def handle_master_commands(self):
        while True:
            try:
                request_type, request_data = self.message_manager.receive_message(self.master_socket)
                if request_type == 'REQUEST':
                    if request_data['Operation'] in ['PREPARE', 'COMMIT', 'ABORT']:
                        # Handle append transaction commands from master
                        response = self.handle_master_append_transaction(request_data)
                        self.message_manager.send_message(self.master_socket, 'RESPONSE', response)
                    if request_data['Operation'] == 'CREATE':
                        file_name = request_data['File_Name']
                        chunk_number = request_data['Chunk_Number']
                        is_primary = request_data['Primary']
                        data = request_data['Data']
                        # Create new chunk
                        if self.chunk_directory.add_chunk(file_name, chunk_number, data, is_primary):
                            response = {'Status': 'SUCCESS'}
                        else:
                            response = {'Status': 'FAILED', 'Error': 'Could not create chunk'}

                    elif request_data['Operation'] == 'DELETE':
                        chunk_id = f"{request_data['File_Name']}_{request_data['Chunk_Number']}"
                        if self.chunk_directory.delete_chunk(chunk_id):
                            response = {'Status': 'SUCCESS'}
                        else:
                            response = {'Status': 'FAILED', 'Error': 'Chunk not found'}
                    self.message_manager.send_message(self.master_socket, 'RESPONSE', response) 
                elif request_type == 'HEARTBEAT_ACK':
                        print("Received heartbeat ACK from master")
                else:
                    print("Unknown request type from master")
                    print("Request: ",request_type," DATA:",request_data)
            except Exception as e:
                print(f"Error handling master command: {e}")
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
                chunk_id = int(file.split('.')[0])
                with open(file, 'rb') as f:
                    data = f.read()
                # Add the chunk to the chunk directory
                self.chunk_directory.add_chunk(chunk_id, '', 0, data)
        os.chdir('..')
        
        
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
            # Send the directory to the master
            self.message_manager.send_message(self.master_socket, 'CHUNK_DIRECTORY',
                                              {'Chunk_Directory': self.chunk_directory.chunk_dict})
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
