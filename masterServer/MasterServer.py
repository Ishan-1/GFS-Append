import socket
import threading
import time
import message  # Custom message manager module

class MasterServer:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.chunkservers = {}  # Store metadata and health status of each chunkserver
        self.chunk_locations = {}  # Track chunks and their assigned servers
        self.primary_chunks = {}  # Track which chunkserver holds the primary role for each chunk
        self.message_manager = message.Message_Manager()
    
    def start(self):
        # Start server socket to accept connections from chunkservers
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.bind((self.host, self.port))
        master_socket.listen(5)
        
        print("Master server started and listening for chunkservers...")
        threading.Thread(target=self.heartbeat_monitor).start()
        
        while True:
            chunkserver_socket, addr = master_socket.accept()
            threading.Thread(target=self.handle_chunkserver, args=(chunkserver_socket,)).start()
    
    def handle_chunkserver(self, chunkserver_socket):
        # Register the chunkserver and receive metadata
        request_type, metadata = self.message_manager.receive_message(chunkserver_socket)
        
        if request_type == 'REGISTER':
            chunkserver_id = metadata['Chunkserver_ID']
            self.chunkservers[chunkserver_id] = {
                'socket': chunkserver_socket,
                'last_heartbeat': time.time(),
                'chunks': metadata['Chunk_Directory']
            }
            print(f"Chunkserver {chunkserver_id} registered with metadata.")
            
            # Store chunks in chunk_locations for easy lookup
            for chunk_id in metadata['Chunk_Directory']:
                self.chunk_locations.setdefault(chunk_id, []).append(chunkserver_id)
    
    def heartbeat_monitor(self):
        # Periodically check chunkserver heartbeats
        while True:
            for chunkserver_id, info in list(self.chunkservers.items()):
                if time.time() - info['last_heartbeat'] > 10:
                    print(f"Chunkserver {chunkserver_id} appears to be down.")
                    del self.chunkservers[chunkserver_id]
            time.sleep(5)
    
    def handle_client_request(self, client_socket, request_type, request_data):
        if request_type == 'CREATE':
            self.handle_create(client_socket, request_data)
        elif request_type == 'DELETE':
            self.handle_delete(client_socket, request_data)
        elif request_type == 'READ':
            self.handle_read(client_socket, request_data)
        elif request_type == 'APPEND':
            self.handle_append(client_socket, request_data)
        else:
            response = {'Status': 'FAILED', 'Error': 'Invalid operation requested'}
            self.message_manager.send_message(client_socket, 'RESPONSE', response)

    def select_chunkservers(self):
        # Simple round-robin or selection logic for demonstration
        return list(self.chunkservers.keys())[:3]  # Select the first 3 chunkservers

    # Define methods for each operation...
    def handle_create(self, client_socket, request_data):
        chunk_id = request_data['Chunk_ID']
        file_name = request_data['File_Name']
        chunk_number = request_data['Chunk_Number']
        
        # Select chunkservers for storing the new chunk
        selected_chunkservers = self.select_chunkservers()
        primary_chunkserver = selected_chunkservers[0] if selected_chunkservers else None
        
        if primary_chunkserver:
            # Store the new chunk on selected chunkservers
            for chunkserver_id in selected_chunkservers:
                self.message_manager.send_message(
                    self.chunkservers[chunkserver_id]['socket'],
                    'REQUEST',
                    {
                        'Operation': 'CREATE',
                        'Chunk_ID': chunk_id,
                        'File_Name': file_name,
                        'Chunk_Number': chunk_number,
                        'Primary': chunkserver_id == primary_chunkserver,
                        'Data': b''  # Placeholder for initial data
                    }
                )
                
            # Update metadata
            self.chunk_locations[chunk_id] = selected_chunkservers
            self.primary_chunks[chunk_id] = primary_chunkserver
            
            response = {
                'Status': 'SUCCESS',
                'Chunk_ID': chunk_id,
                'Chunkservers': selected_chunkservers,
                'Primary': primary_chunkserver
            }
        else:
            response = {'Status': 'FAILED', 'Error': 'No chunkserver available for CREATE'}
        
        self.message_manager.send_message(client_socket, 'RESPONSE', response)
        
        
    def handle_delete(self, client_socket, request_data):
        file_name = request_data['File_Name']
        chunk_id = request_data['Chunk_ID']

        if chunk_id in self.chunk_locations:
            # Retrieve chunkservers storing this chunk
            chunkservers = self.chunk_locations[chunk_id]
            for chunkserver_id in chunkservers:
                # Send DELETE request to each chunkserver
                self.message_manager.send_message(
                    self.chunkservers[chunkserver_id]['socket'],
                    'REQUEST',
                    {
                        'Operation': 'DELETE',
                        'Chunk_ID': chunk_id,
                        'File_Name': file_name
                    }
                )
            # Update metadata after deletion
            del self.chunk_locations[chunk_id]
            del self.primary_chunks[chunk_id]

            response = {
                'Status': 'SUCCESS',
                'Message': f"Deleted chunk {chunk_id} across all replicas"
            }
        else:
            response = {
                'Status': 'FAILED',
                'Error': "Chunk ID not found"
            }

        self.message_manager.send_message(client_socket, 'RESPONSE', response)

    def handle_read(self, client_socket, request_data):
        file_name = request_data['File_Name']
        chunk_id = request_data['Chunk_ID']

        if chunk_id in self.chunk_locations:
            # Get chunkservers and primary details
            chunkservers = self.chunk_locations[chunk_id]
            primary_chunkserver = self.primary_chunks.get(chunk_id)
            response = {
                'Status': 'SUCCESS',
                'Chunkservers': chunkservers,
                'Primary': primary_chunkserver
            }
        else:
            response = {
                'Status': 'FAILED',
                'Error': "Chunk ID not found"
            }

        self.message_manager.send_message(client_socket, 'RESPONSE', response)

    def handle_append(self, client_socket, request_data):
        file_name = request_data['File_Name']
        chunk_id = request_data['Chunk_ID']
        data = request_data['Data']

        if chunk_id in self.chunk_locations:
            chunkservers = self.chunk_locations[chunk_id]
            primary_chunkserver = self.primary_chunks.get(chunk_id)
            
            # First, send APPEND request to the primary chunkserver
            success = False
            for chunkserver_id in chunkservers:
                is_primary = (chunkserver_id == primary_chunkserver)
                self.message_manager.send_message(
                    self.chunkservers[chunkserver_id]['socket'],
                    'REQUEST',
                    {
                        'Operation': 'APPEND',
                        'Chunk_ID': chunk_id,
                        'File_Name': file_name,
                        'Data': data,
                        'Primary': is_primary
                    }
                )
                # Await response and confirm success from primary chunkserver
                response_type, response_data = self.message_manager.receive_message(self.chunkservers[chunkserver_id]['socket'])
                if response_type == 'RESPONSE' and response_data['Status'] == 'SUCCESS' and is_primary:
                    success = True
                    break
            
            if success:
                response = {
                    'Status': 'SUCCESS',
                    'Chunkservers': chunkservers,
                    'Primary': primary_chunkserver
                }
            else:
                response = {
                    'Status': 'FAILED',
                    'Error': "Primary chunkserver failed to append"
                }
        else:
            response = {
                'Status': 'FAILED',
                'Error': "Chunk ID not found"
            }

        self.message_manager.send_message(client_socket, 'RESPONSE', response)


    def handle_heartbeat(self, chunkserver_id):
        self.chunkservers[chunkserver_id]['last_heartbeat'] = time.time()
        print(f"Heartbeat received from chunkserver {chunkserver_id}.")


if __name__ == "__main__":
    master_server = MasterServer()
    master_server.start()


