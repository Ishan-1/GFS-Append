import os
class Chunk:
    def __init__(self,chunk_id:int,file_name:str,version:int,chunk_number:int,data):
        self.chunk_id = chunk_id
        self.version = version
        # Construct chunk filename
        self.chunk_filename = f"{self.file_name}_{self.chunk_number}_{self.version}"
        # Write data to chunk file if it doesn't exist
        if not os.path.exists(self.chunk_filename):    
          try:
            with open(self.chunk_filename, 'wb') as f:
                f.write(data)
          except:
            print("Error creating chunk file")
    
    def delete(self):
        os.remove(self.chunk_filename)
        return not (os.path.exists(self.chunk_filename))
    
    def read(self):
        try:
         with self.chunk_filename as f:
            return f.read()
        except:
            print("Error reading chunk file")
            return None
        
    def append(self,data):
       try:
           with open(self.chunk_filename, 'ab') as f:
              f.write(data)
              # Rename chunk file to reflect new version
              os.rename(self.chunk_filename, f"{self.file_name}_{self.chunk_number}_{self.version+1}")
              self.version += 1
           return True
       except:
              print("Error appending to chunk file")
              return False
    
    def get_chunk_id(self):
        return self.chunk_id
    