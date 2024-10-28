import GFS_chunk

class Chunk_Directory:
    def __init__(self):
        self.chunk_dict = {}
    
    
    def add_chunk(self,chunk_id:int,file_name:str,version:int,chunk_number:int,data,primary=False):
        self.chunk_dict[chunk_id] = {'Chunk':GFS_chunk.Chunk(chunk_id,file_name,version,chunk_number,data),
                                     'Version':version,
                                     'Primary':primary}
        return True
        
        
        
    def get_chunk(self,chunk_id:int):
        return self.chunk_dict[chunk_id]['Chunk']
    
        
    