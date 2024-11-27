def handle_master_append_transaction(self, request_data):
        """Handle a master append transaction with optimized chunk allocation."""
        transaction_id = request_data['Transaction_ID']
        operation = request_data['Operation']
        file_name = request_data['File_Name']
        chunk_number = request_data['Chunk_Number'] 
        chunk_id = f"{file_name}_{chunk_number}"
        is_primary = request_data.get('Primary', False)

        print(f"[DEBUG] Received {operation} operation for Transaction_ID {transaction_id}, Chunk_ID {chunk_id}")
        print(f"[DEBUG] Full request data: {request_data}")

        try:
            if operation == 'PREPARE':
                print(f"[DEBUG] Handling PREPARE operation for Transaction_ID {transaction_id}")
                data = request_data.get('Data', '')
                data_length = request_data.get('Data_Length', 0)

                # If this is the primary chunkserver, check if data can fit in the last chunk
                if is_primary:
                    try:
                        # Get the last chunk for this file
                        last_chunk = self.chunk_directory.get_chunk(chunk_id)
                        if last_chunk:
                            remaining_space = last_chunk.get_remaining_space()
                            print(f"[DEBUG] Last chunk {chunk_id} has {remaining_space} bytes remaining")

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
                        print(f"[DEBUG] Error checking last chunk: {str(chunk_error)}")

                # Store transaction information for later commit
                self.append_transactions[transaction_id] = {
                    'Chunk_ID': chunk_id,
                    'File_Name': file_name,
                    'Data': data,
                    'Status': 'PREPARED',
                    'Is_Primary': is_primary
                }

                print(f"[DEBUG] Transaction {transaction_id} prepared successfully for Chunk_ID {chunk_id}")
                return {'Operation':'PREPARE','Status': 'READY'}

            elif operation == 'COMMIT':
                print(f"[DEBUG] Handling COMMIT operation for Transaction_ID {transaction_id}")
                
                if transaction_id not in self.append_transactions:
                    print(f"[DEBUG] COMMIT failed: Unknown Transaction_ID {transaction_id}")
                    return {'Operation':'PREPARE','Status': 'FAILED', 'Reason': 'Unknown transaction'}

                transaction = self.append_transactions[transaction_id]
                chunk = self.chunk_directory.get_chunk(transaction['Chunk_ID'])

                if not chunk:
                    chunk = self.chunk_directory.add_chunk(
                        transaction['File_Name'],
                        chunk_number,
                        transaction['Data'],
                        transaction['Is_Primary']
                    )
                    if not chunk:
                        return {'Status': 'FAILED', 'Reason': 'Could not create chunk'}
                else:
                    try:
                        print(transaction['Data'])
                        self.chunk_directory.append_chunk(transaction['Chunk_ID'], transaction['Data'])
                    except Exception as append_error:
                        print(f"[DEBUG] Error appending data: {str(append_error)}")
                        return {'Status': 'FAILED', 'Reason': f'Error appending data: {str(append_error)}'}

                print(f"[DEBUG] Data appended to Chunk_ID {transaction['Chunk_ID']} for Transaction_ID {transaction_id}")
                del self.append_transactions[transaction_id]
                return {'Operation':'COMMIT','Status': 'SUCCESS','Transaction_ID':transaction_id}

            elif operation == 'ABORT':
                print(f"[DEBUG] Handling ABORT operation for Transaction_ID {transaction_id}")
                if transaction_id in self.append_transactions:
                    del self.append_transactions[transaction_id]
                    print(f"[DEBUG] Transaction {transaction_id} aborted successfully")
                return {'Operation':'ABORT','Status': 'ABORTED','Transaction_ID':transaction_id}

        except Exception as e:
            print(f"[DEBUG] Exception occurred while handling {operation} for Transaction_ID {transaction_id}")
            print(f"[DEBUG] Exception details: {str(e)}")
            return {'Status': 'FAILED', 'Reason': str(e)}