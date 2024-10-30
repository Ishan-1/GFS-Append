import os
import GFS_chunk
import GFS_chunk_metadata
import socket

def start_chunkserver():
    # Create a directory for the chunkserver
    os.makedirs('chunkserver',exist_ok=True)
    # Jump into the chunkserver directory
    os.chdir('chunkserver')
    # Create a chunk directory for the chunkserver
    chunk_directory = GFS_chunk_metadata.Chunk_Directory()
    # Load the chunk directory from disk by looking for chunk files
    for file in os.listdir():
        if file.endswith('.chunk'):
            # Load the chunk file
            chunk_id = int(file.split('.')[0])
            with open(file,'rb') as f:
                data = f.read()
            # Add the chunk to the chunk directory
            chunk_directory.add_chunk(chunk_id,'',0,data)
    # Connect to the master on port 5000
    connection_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    socket.create_connection(('localhost',5000))
    # Register with the master
    connection_socket.send('REGISTER'.encode())
    # Receive the master's response
    response = connection_socket.recv(1024).decode()
    # Check if the master accepted the registration
    if response == 'ACCEPTED':
        # Start the heartbeat
        
    else:
        # Close the connection
        connection_socket.close()
        # Exit the chunkserver
        return
    # Start the heartbeat
    # Start the chunkserver
    while True:
        pass
