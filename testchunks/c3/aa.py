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


    def handle_master_append_transaction(self, request_data):
        """Handle a master append transaction."""
        transaction_id = request_data['Transaction_ID']
        operation = request_data['Operation']
        chunk_id = f"{request_data['File_Name']}_{request_data['Chunk_Number']}"

        print(f"[DEBUG] Received {operation} operation for Transaction_ID {transaction_id}, Chunk_ID {chunk_id}")
        print(f"[DEBUG] Full request data: {request_data}")  # Add this line to see complete request data

        try:
            if operation == 'PREPARE':
                print(f"[DEBUG] Handling PREPARE operation for Transaction_ID {transaction_id}")
                
                # Debug chunk directory state
                print(f"[DEBUG] Current chunk directory state: {self.chunk_directory}")
                print(f"[DEBUG] Looking up chunk: {chunk_id}")
                try:
                    self.append_transactions[transaction_id] = {
                        'Chunk_ID': chunk_id,
                        'File_Name': request_data['File_Name'],
                        'Data': request_data['Data'],
                        'Status': 'PREPARED'
                    }
                except Exception as tx_error:
                    print(f"[DEBUG] Error storing transaction: {str(tx_error)}")
                    return {'Status': 'FAILED', 'Reason': f'Error storing transaction: {str(tx_error)}'}

                print(f"[DEBUG] Transaction {transaction_id} prepared successfully for Chunk_ID {chunk_id}")
                return {'Status': 'READY'}

            # Rest of the code remains the same...
            elif operation == 'COMMIT':
                print(f"[DEBUG] Handling COMMIT operation for Transaction_ID {transaction_id}")
                
                # Verify transaction exists
                if transaction_id not in self.append_transactions:
                    print(f"[DEBUG] COMMIT failed: Unknown Transaction_ID {transaction_id}")
                    return {'Status': 'FAILED', 'Reason': 'Unknown transaction'}

                transaction = self.append_transactions[transaction_id]
                
                try:
                    chunk = self.chunk_directory.get_chunk(transaction['Chunk_ID'])
                except Exception as chunk_error:
                    print(f"[DEBUG] Error retrieving chunk during COMMIT: {str(chunk_error)}")
                    return {'Status': 'FAILED', 'Reason': f'Error accessing chunk: {str(chunk_error)}'}

                if not chunk:
                    print(f"[DEBUG] COMMIT failed: Chunk {transaction['Chunk_ID']} not found")
                    return {'Status': 'FAILED', 'Reason': 'Chunk not found'}

                # Append data to chunk
                try:
                    chunk.append(transaction['Data'])
                except Exception as append_error:
                    print(f"[DEBUG] Error appending data: {str(append_error)}")
                    return {'Status': 'FAILED', 'Reason': f'Error appending data: {str(append_error)}'}

                print(f"[DEBUG] Data appended to Chunk_ID {transaction['Chunk_ID']} for Transaction_ID {transaction_id}")

                # Clean up transaction
                del self.append_transactions[transaction_id]
                print(f"[DEBUG] Transaction {transaction_id} committed and cleaned up successfully")
                return {'Status': 'SUCCESS'}

            elif operation == 'ABORT':
                print(f"[DEBUG] Handling ABORT operation for Transaction_ID {transaction_id}")
                
                # Remove transaction if it exists
                if transaction_id in self.append_transactions:
                    del self.append_transactions[transaction_id]
                    print(f"[DEBUG] Transaction {transaction_id} aborted successfully")
                else:
                    print(f"[DEBUG] No transaction found to abort for Transaction_ID {transaction_id}")
                return {'Status': 'ABORTED'}

        except Exception as e:
            print(f"[DEBUG] Exception occurred while handling {operation} for Transaction_ID {transaction_id}")
            print(f"[DEBUG] Exception type: {type(e)}")
            print(f"[DEBUG] Exception details: {str(e)}")
            print(f"[DEBUG] Stack trace:", exc_info=True)
            return {'Status': 'FAILED', 'Reason': str(e)}


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
                        print(f"[DEBUG] Response for {operation} operation: {response}")
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
                            print(f"[DEBUG] No files found matching pattern {base_chunk_id}_*.chunk")
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
                                        print(f"[DEBUG] Removed {chunk_id} from chunk directory")
                                except OSError as e:
                                    print(f"[DEBUG] Failed to delete file {file}: {str(e)}")
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
                print(f"[DEBUG] Error handling master command: {e}")
                break

    


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 GFS_chunkserver.py port directory")
        sys.exit(1)
    port=int(sys.argv[1])
    directory=sys.argv[2]
    chunkserver = ChunkServer(port, directory)
    chunkserver.start_chunkserver()
