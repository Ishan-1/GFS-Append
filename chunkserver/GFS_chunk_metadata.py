import GFS_chunk

class Chunk_Directory:
    def __init__(self):
        # Store chunk information
        self.chunk_dict = {}
        # Store lease information for primary chunks
        self.lease_dict = {}
        # Lease duration in seconds
        self.lease_duration = 300
       
    def add_chunk(self,chunk_id:int,file_name:str,chunk_number:int,data,primary=False):
        self.chunk_dict[chunk_id] = {'Chunk':GFS_chunk.Chunk(chunk_id,file_name,1,chunk_number,data),
                                     'Version':1,
                                     'Primary':primary}
        if primary:  
          self.lease_dict[chunk_id] = self.lease_duration 
        return True
    
    
        
    def delete_chunk(self,chunk_id:int):
        self.chunk_dict[chunk_id]['Chunk'].delete()
        return self.chunk_dict.pop(chunk_id,None)
    
    def append_chunk(self,chunk_id:int,data):
        self.chunk_dict[chunk_id]['Chunk'].append(data)
        self.chunk_dict[chunk_id]['Version'] += 1
        return True
        
    def get_chunk(self,chunk_id:int):
        return self.chunk_dict[chunk_id]['Chunk']
            
    