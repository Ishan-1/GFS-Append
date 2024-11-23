import os

class Chunk:
    def __init__(self, file_name: str, version: int, chunk_number: int, data: bytes):
        self.version = version
        self.file_name = file_name
        self.chunk_number = chunk_number
        # Construct chunk filename
        self.chunk_filename = f"{file_name}_{chunk_number}_{version}"
        
        # Write data to chunk file if it doesn't exist
        if not os.path.exists(self.chunk_filename):
            try:
                with open(self.chunk_filename, 'wb') as f:
                    f.write(data)
            except Exception as e:
                print("Error creating chunk file:", e)
    
    def delete(self):
        os.remove(self.chunk_filename)
        return not os.path.exists(self.chunk_filename)
    
    def read(self,start,end):
        try:
            with open(self.chunk_filename, 'rb') as f:
                f.seek(start)
                return f.read(end-start)
        except Exception as e:
            return f"Error reading chunk file: {e}"
        
    def append(self, data: bytes):
        try:
            with open(self.chunk_filename, 'ab') as f:
                f.write(data)
            # Rename chunk file to reflect new version
            new_filename = f"{self.file_name}_{self.chunk_number}_{self.version + 1}"
            os.rename(self.chunk_filename, new_filename)
            self.chunk_filename = new_filename
            self.version += 1
            return True
        except Exception as e:
            return f"Error appending to chunk file: {e}"
    
    def get_chunk_id(self):
        return self.chunk_id
