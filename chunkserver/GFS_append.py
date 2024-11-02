import GFS_chunk
import GFS_chunk_metadata
import socket
import message
import json
import time
import threading
from enum import Enum

class AppendState(Enum):
    INIT = 0
    PREPARED = 1
    COMMITTED = 2
    ABORTED = 3


class PendingAppend:
    def __init__(self, chunk_id, data):
        self.chunk_id = chunk_id
        self.data = data
        self.state = AppendState.INIT
        self.prepared_offset = None



pending_appends = {} 

def cleanup_client_appends():
    # Find and remove any pending appends associated with this client socket
    for transaction_id in list(pending_appends.keys()):
        pending_append = pending_appends[transaction_id]
        if pending_append.state != AppendState.COMMITTED:
            del pending_appends[transaction_id]



def handle_prepare_append(client_socket, message_manager, request_data, chunkserver_id, chunk_directory):
    """Handle the prepare phase of 2PC initiated by client"""
    transaction_id = request_data.get('Transaction_ID')
    chunk_id = request_data.get('Chunk_ID')
    data = request_data.get('Data')
    
    if not all([transaction_id, chunk_id, data]):
        response = {'Status': 'FAILED', 'Error': 'Missing required parameters'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return
        
    chunk = chunk_directory.get_chunk(chunk_id)
    if chunk is None:
        response = {'Status': 'FAILED', 'Error': 'Chunk not found'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    # Verify we have enough space for the append
    if chunk.size + len(data) > chunk.max_size:
        response = {'Status': 'FAILED', 'Error': 'Insufficient space'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    # Create pending append entry
    pending_append = PendingAppend(chunk_id, data)
    pending_append.state = AppendState.PREPARED
    pending_append.prepared_offset = chunk.size
    pending_appends[transaction_id] = pending_append

    # Send PREPARED response with current offset
    response = {
        'Status': 'PREPARED',
        'Transaction_ID': transaction_id,
        'Chunkserver_ID': chunkserver_id,
        'Prepared_Offset': chunk.size
    }
    message_manager.send_message(client_socket, 'RESPONSE', response)


def handle_commit_append(client_socket, message_manager, request_data, chunk_directory):
    """Handle the commit phase of 2PC initiated by client"""
    transaction_id = request_data.get('Transaction_ID')
    chunk_id = request_data.get('Chunk_ID')
    commit_offset = request_data.get('Commit_Offset')
    
    if not all([transaction_id, chunk_id, commit_offset]):
        response = {'Status': 'FAILED', 'Error': 'Missing required parameters'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    pending_append = pending_appends.get(transaction_id)
    if not pending_append or pending_append.chunk_id != chunk_id:
        response = {'Status': 'FAILED', 'Error': 'Invalid transaction'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    if pending_append.state != AppendState.PREPARED:
        response = {'Status': 'FAILED', 'Error': 'Transaction not prepared'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    chunk = chunk_directory.get_chunk(chunk_id)
    
    # Verify the commit offset matches our prepared offset
    if commit_offset != pending_append.prepared_offset:
        response = {
            'Status': 'FAILED', 
            'Error': f'Offset mismatch. Expected: {pending_append.prepared_offset}, Got: {commit_offset}'
        }
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    try:
        # Perform the actual append
        chunk.append(pending_append.data, commit_offset)
        pending_append.state = AppendState.COMMITTED
        
        response = {
            'Status': 'SUCCESS',
            'Transaction_ID': transaction_id,
            'Offset': commit_offset,
            'Size': len(pending_append.data)
        }
        
    except Exception as e:
        response = {'Status': 'FAILED', 'Error': str(e)}
    
    message_manager.send_message(client_socket, 'RESPONSE', response)
    
    # Clean up the committed transaction
    if response['Status'] == 'SUCCESS':
        del pending_appends[transaction_id]


def handle_abort_append(client_socket, message_manager, request_data):
    """Handle the abort command from client"""
    transaction_id = request_data.get('Transaction_ID')
    chunk_id = request_data.get('Chunk_ID')
    
    if not transaction_id or not chunk_id:
        response = {'Status': 'FAILED', 'Error': 'Missing required parameters'}
        message_manager.send_message(client_socket, 'RESPONSE', response)
        return

    pending_append = pending_appends.get(transaction_id)
    if pending_append and pending_append.chunk_id == chunk_id:
        pending_append.state = AppendState.ABORTED
        del pending_appends[transaction_id]
        
    response = {'Status': 'SUCCESS', 'Transaction_ID': transaction_id}
    message_manager.send_message(client_socket, 'RESPONSE', response)