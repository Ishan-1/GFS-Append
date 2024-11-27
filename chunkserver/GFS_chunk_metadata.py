import GFS_chunk

class Chunk_Directory:
    def __init__(self):
        # Store chunk information
        self.chunk_dict = {}
        # Store lease information for primary chunks
        self.lease_dict = {}
        # Lease duration in seconds
        self.lease_duration = 300
       
    def add_chunk(self,file_name:str,chunk_number:int,data,primary=False):
        chunk_id=f'{file_name}_{chunk_number}'
        self.chunk_dict[chunk_id] = {'Chunk':GFS_chunk.Chunk(file_name,1,chunk_number,data),
                                     'Version':1,
                                     'Primary':primary}
        self.chunk_dict[chunk_id]['Chunk'].write(data)
        if primary:  
          self.lease_dict[chunk_id] = self.lease_duration 
        return True
    
    def load_chunk(self,file_name:str,chunk_number:int,version,data):
        chunk_id=f'{file_name}_{chunk_number}'
        self.chunk_dict[chunk_id] = {'Chunk':GFS_chunk.Chunk(file_name,version,chunk_number,data),
                                     'Version':version,
                                     'Primary':False} 
        return True
    def delete_chunk(self,chunk_id):
        self.chunk_dict[chunk_id]['Chunk'].delete()
        return self.chunk_dict.pop(chunk_id,None)
    
    def append_chunk(self,chunk_id,data):
        self.chunk_dict[chunk_id]['Chunk'].append(data)
        self.chunk_dict[chunk_id]['Version'] += 1
        return True
        
    def get_chunk(self,chunk_id):
        if chunk_id in self.chunk_dict:
            return self.chunk_dict[chunk_id]['Chunk']
        return None
            
    