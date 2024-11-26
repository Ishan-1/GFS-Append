   # def handle_append(self, conn, file_name, data_length):
    #     """Handle APPEND operation."""
    #     transaction_id = str(uuid.uuid4())
    #     print(f"[DEBUG] Starting APPEND operation for file: {file_name}, data_length: {data_length}")

    #     num_chunks = max(1, math.ceil(data_length / CHUNK_SIZE))
    #     chunks = []
    #     next_chunk_number = max(self.parse_chunk_id(chunk_id)[1] for chunk_id in self.file_chunks.get(file_name, []))
    #     for _ in range(num_chunks):
    #         with self.lock:
    #             # Iterate through chunk numbers for the file to find the next available chunk number
    #             chunk_number = next_chunk_number + 1
    #             next_chunk_number += 1
    #             chunk_id = self.generate_chunk_id(file_name, chunk_number)

    #         selected_server_ids = self.select_chunkservers()
    #         if not selected_server_ids:
    #             print(f"[ERROR] Insufficient chunkservers available for Chunk ID: {chunk_id}")
    #             response = {'Status': 'FAILED', 'Error': 'Insufficient chunkservers available'}
    #             self.message_manager.send_message(conn, 'RESPONSE', response)
    #             return

    #         self.chunk_locations[chunk_id] = selected_server_ids
    #         chunks.append({
    #             'Chunk_ID': chunk_id,
    #             'Chunkserver_IDs': selected_server_ids,
    #             'Chunkservers': [self.chunkservers[cs_id]['address'] for cs_id in selected_server_ids]
    #         })

    #     self.append_transactions[transaction_id] = {
    #         'File_Name': file_name,
    #         'Chunks': chunks,
    #         'Status': 'PREPARE'
    #     }

    #     prepare_responses = []
    #     for chunk in chunks:
    #         for cs_id in chunk['Chunkserver_IDs']:
    #             response = self.send_prepare_to_chunkserver(
    #                 cs_id, transaction_id, chunk['Chunk_ID'], data_length, file_name
    #             )
    #             prepare_responses.append(response)

    #     if all(resp.get('Status') == 'READY' for resp in prepare_responses):
    #         commit_responses = []
    #         for chunk in chunks:
    #             for cs_id in chunk['Chunkserver_IDs']:
    #                 response = self.send_commit_to_chunkserver(
    #                     cs_id, transaction_id, chunk['Chunk_ID']
    #                 )
    #                 commit_responses.append(response)

    #         if all(resp.get('Status') == 'SUCCESS' for resp in commit_responses):
    #             with self.lock:
    #                 self.file_chunks.setdefault(file_name, []).extend(
    #                     chunk['Chunk_ID'] for chunk in chunks
    #                 )
                
    #             response = {
    #                 'Status': 'SUCCESS',
    #                 'Chunks': [{
    #                     'Chunk_ID': chunk['Chunk_ID'],
    #                     'Chunkservers': chunk['Chunkservers']
    #                 } for chunk in chunks],
    #                 'Replicas': self.replicas,
    #                 'Transaction_ID': transaction_id
    #             }
    #         else:
    #             self.abort_append_transaction(transaction_id)
    #             response = {'Status': 'FAILED', 'Error': 'Commit failed'}
    #     else:
    #         self.abort_append_transaction(transaction_id)
    #         response = {'Status': 'FAILED', 'Error': 'Prepare phase failed'}

    #     with self.lock:
    #         if transaction_id in self.append_transactions:
    #             del self.append_transactions[transaction_id]

    #     self.message_manager.send_message(conn, 'RESPONSE', response)

    # def send_prepare_to_chunkserver(self, chunkserver_id, transaction_id, chunk_id, data_length, file_name):
    #     """Send PREPARE message to a specific chunkserver."""
    #     chunk_number = self.parse_chunk_id(chunk_id)[1]
    #     prepare_request = {
    #         'Operation': 'PREPARE',
    #         'Transaction_ID': transaction_id,
    #         'Chunk_Number': chunk_number,
    #         'Data_Length': data_length,
    #         'File_Name': file_name
    #     }
    #     response = self.send_to_chunkserver(chunkserver_id, 'REQUEST', prepare_request)
    #     return response