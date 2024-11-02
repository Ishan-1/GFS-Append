import os
import GFS_chunk
import GFS_chunk_metadata
import socket
import message
import json
import time
import threading
from GFS_append import cleanup_client_appends, handle_prepare_append, handle_commit_append, handle_abort_append


chunkserver_id = 0
chunk_directory = GFS_chunk_metadata.Chunk_Directory()

def send_heartbeat(message_manager, connection_socket):
    sleep_time = 5
    while True:
        try:
           message_manager.send_message(connection_socket, 'HEARTBEAT', {'Operation': 'HEARTBEAT'})
        except:
            print("Error sending heartbeat")
        finally:
          time.sleep(sleep_time)


def handle_read(client_socket, message_manager, request_data):
    chunk_id = request_data['Chunk_ID']
    chunk = chunk_directory.get_chunk(chunk_id)
    if chunk is None:
        response = {'Status': 'FAILED', 'Error': 'Chunk not found'}
    else:
        response = {'Status': 'SUCCESS', 'Data': chunk.read()}
    message_manager.send_message(client_socket, 'RESPONSE', response)



def client_thread(client_socket):
    client_message_manager = message.Message_Manager()
    try:
        while True:  # Keep connection alive for the entire 2PC protocol
            request_type, request_data = client_message_manager.receive_message(client_socket)
            if not request_type:  # Connection closed by client
                break
                
            if request_type == 'REQUEST':
                if request_data['Operation'] == 'READ':
                    handle_read(client_socket, client_message_manager, request_data)
                elif request_data['Operation'] == 'PREPARE_APPEND':
                    handle_prepare_append(client_socket, client_message_manager, request_data, chunkserver_id, chunk_directory)
                elif request_data['Operation'] == 'COMMIT_APPEND':
                    handle_commit_append(client_socket, client_message_manager, request_data, chunk_directory)
                elif request_data['Operation'] == 'ABORT_APPEND':
                    handle_abort_append(client_socket, client_message_manager, request_data)
    finally:
        # Clean up any pending appends from this client
        cleanup_client_appends()
        client_socket.close()


            
def update_leases(connection_socket):
    message_manager = message.Message_Manager()
    while True:
        for chunk_id, lease in list(chunk_directory.lease_dict.items()):
            chunk_directory.lease_dict[chunk_id]['Time'] -= 1
            if lease['Time'] == 5:
                # Renew the lease
                message_manager.send_message(connection_socket, 'REQUEST', 
                                          {'Operation': 'RENEW_LEASE', 'Chunk_ID': chunk_id})
                response, data = message_manager.receive_message(connection_socket)
                if response == 'RESPONSE':
                    if data['Status'] == 'SUCCESS':
                        chunk_directory.lease_dict[chunk_id]['Time'] = 60  # Reset lease time
                    else:
                        # If lease renewal fails, remove the lease
                        del chunk_directory.lease_dict[chunk_id]
        time.sleep(1)  # Check leases every second
            
            
def client_chunkserver_thread():
    message_manager = message.Message_Manager()
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 5000 + chunkserver_id))
    server_socket.listen(5)
    
    # Accept client connections
    while True:
        client_socket, address = server_socket.accept()
        # Start a new thread to handle the client request
        client_handler = threading.Thread(target=client_thread, args=(client_socket,))
        client_handler.start()


def handle_master_commands(connection_socket):
    message_manager = message.Message_Manager()
    while True:
        try:
            request_type, request_data = message_manager.receive_message(connection_socket)
            if request_type == 'REQUEST':
                if request_data['Operation'] == 'CREATE':
                    chunk_id = request_data['Chunk_ID']
                    file_name = request_data['File_Name']
                    chunk_number = request_data['Chunk_Number']
                    is_primary = request_data['Primary']
                    data = request_data['Data']
                    # Create new chunk
                    if chunk_directory.add_chunk(chunk_id,file_name,chunk_number,data,is_primary):
                       response = {'Status': 'SUCCESS'}
                    else:
                          response = {'Status': 'FAILED', 'Error': 'Could not create chunk'}
                    
                elif request_data['Operation'] == 'DELETE':
                    chunk_id = request_data['Chunk_ID']
                    if chunk_directory.delete_chunk(chunk_id):
                        response = {'Status': 'SUCCESS'}
                    else:
                        response = {'Status': 'FAILED', 'Error': 'Chunk not found'}
                        
                message_manager.send_message(connection_socket, 'RESPONSE', response)
        except Exception as e:
            print(f"Error handling master command: {e}")
            break


def start_chunkserver():
    global chunkserver_id
    
    # Create a directory for the chunkserver
    os.makedirs('chunkserver', exist_ok=True)
    
    # Jump into the chunkserver directory
    os.chdir('chunkserver')
    
    # Load the chunk directory from disk by looking for chunk files
    for file in os.listdir():
        if file.endswith('.chunk'):
            # Load the chunk file
            chunk_id = int(file.split('.')[0])
            with open(file, 'rb') as f:
                data = f.read()
            # Add the chunk to the chunk directory
            chunk_directory.add_chunk(chunk_id, '', 0, data)
            
    # Connect to the master on port 5000
    connection_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connection_socket.connect(('localhost', 5000))
    except ConnectionRefusedError:
        print("Failed to connect to master server")
        return
        
    # Register with the master
    message_manager = message.Message_Manager()
    message_manager.send_message(connection_socket, 'REQUEST', 
                               {'Operation': 'REGISTER', 'Chunk_Directory': chunk_directory.chunk_dict})
    
    # Receive the master's response
    response, data = message_manager.receive_message(connection_socket)
    
    # Check if the master accepted the registration
    if response == 'RESPONSE' and data['Status'] == 'SUCCESS':
        chunkserver_id = data['Chunkserver_ID']
        
        # Create threads for various operations
        heartbeat_thread = threading.Thread(target=send_heartbeat, 
                                         args=(message_manager, connection_socket))
        lease_thread = threading.Thread(target=update_leases, 
                                     args=(connection_socket,))
        client_server_thread = threading.Thread(target=client_chunkserver_thread)
        master_command_thread = threading.Thread(target=handle_master_commands, 
                                              args=(connection_socket,))
        
        # Start all threads
        heartbeat_thread.daemon = True
        lease_thread.daemon = True
        client_server_thread.daemon = True
        master_command_thread.daemon = True
        
        heartbeat_thread.start()
        lease_thread.start()
        client_server_thread.start()
        master_command_thread.start()
        
        print(f"Chunkserver {chunkserver_id} started successfully")
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down chunkserver...")
    else:
        print("Registration with master failed")
        connection_socket.close()
        return


if __name__ == "__main__":
    start_chunkserver()