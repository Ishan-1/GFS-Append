import socket
import threading
import time
import json
from config import MASTER_SERVER_PORT

class DummyChunkServer:
    def __init__(self, host='localhost', port=0):
        self.host = host
        self.port = port
        self.server_socket = None
        self.running = False
    
    def start(self):
        """Start the dummy chunkserver and register it with the master server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.port = self.server_socket.getsockname()[1]  # Get the assigned port number
        print(f"Dummy chunkserver started on {self.host}:{self.port}")
        
        # Start a separate thread to handle master connections
        threading.Thread(target=self.handle_connections, daemon=True).start()
        
        # Register with the master server
        self.register_with_master()
    
    def register_with_master(self):
        """Send a registration request to the master server."""
        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect(('localhost', MASTER_SERVER_PORT))  # Adjust the MASTER_SERVER_PORT as needed
            
            # Register message
            register_data = {
                'Address': [self.host, self.port]
            }
            message = json.dumps({'Type': 'REGISTER', 'Data': register_data})
            master_socket.sendall(message.encode())
            
            # Wait for a response
            response = master_socket.recv(1024).decode()
            print(f"Received response from master: {response}")
            
            master_socket.close()
        except ConnectionError:
            print("Failed to connect to the master server for registration.")
    
    def handle_connections(self):
        """Handle incoming requests, such as heartbeat or data storage requests."""
        while True:
            client_socket, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_request, args=(client_socket,), daemon=True).start()
    
    def handle_request(self, conn):
        """Handle dummy requests from the master."""
        try:
            request_data = conn.recv(1024).decode()
            print(f"Received request: {request_data}")
            
            # Send a dummy response to simulate success
            response = json.dumps({'Type': 'RESPONSE', 'Data': {'Status': 'SUCCESS'}})
            conn.sendall(response.encode())
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            conn.close()

# Run the dummy chunkserver
if __name__ == "__main__":
    chunkserver = DummyChunkServer()
    chunkserver.start()
    while True:
        time.sleep(1)  # Keep the main thread alive
