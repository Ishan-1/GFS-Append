import os

class Chunk:
    def __init__(self, file_name: str, version: int, chunk_number: int, data):
        self.version = version
        self.file_name = file_name
        self.chunk_number = chunk_number
        self.data = data
        self.chunk_filename = f"{self.file_name}_{self.chunk_number}_{self.version}.chunk"
        
    
    def write(self, data):
        # Write data to chunk file if it doesn't exist
        if not os.path.exists(self.chunk_filename):
            try:
                with open(self.chunk_filename, 'w') as f:
                    f.write(data)
            except Exception as e:
                print("Error creating chunk file:", e)
                return False
        return True
    
    def delete(self):
        os.remove(self.chunk_filename)
        return not os.path.exists(self.file_name)
    
    def read(self,start,end):
        try:
            with open(self.chunk_filename, 'r') as f:
                f.seek(start)
                return f.read(end-start)
        except Exception as e:
            return f"Error reading chunk file: {e}"
        
    def append(self, data):
        try:
            with open(self.chunk_filename, 'a') as f:
                f.write(data)
            # Rename chunk file to reflect new version
            new_filename = f"{self.file_name}_{self.chunk_number}_{self.version + 1}.chunk"
            os.rename(self.chunk_filename, new_filename)
            self.chunk_filename = new_filename
            self.version += 1
            return True
        except Exception as e:
            return f"Error appending to chunk file: {e}"
    
    def get_chunk_info(self):
        return f'{self.file_name},{self.chunk_number}'
    
    def get_remaining_space(self):
        # Read number of characters in chunk file
        try:
            with open(self.chunk_filename, 'r') as f:
                space_used = len(f.read())
                return 64 - space_used
        except Exception as e:
            return f"Error reading chunk file: {e}"