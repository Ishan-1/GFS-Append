from dataclasses import dataclass
from typing import List, Tuple, Dict

@dataclass
class ChunkLocation:
    chunk_handle: str
    chunk_servers: List[str]
    primary: str = None
    secondary: List[str] = None
    
@dataclass
class ChunkInfo:
    chunk_handle: str
    offset: int
    size: int

class GFSClient:
    CHUNK_SIZE = 64 * 1024 * 1024  # 64MB in bytes
    
    def __init__(self, master_address: str):
        self.master_address = master_address
        self.metadata_cache = {}  # Cache for chunk locations
        self.lease_holders = {}   # Cache for lease holders
        
    def _get_chunk_index(self, offset: int) -> int:
        """Calculate chunk index from offset"""
        return offset // self.CHUNK_SIZE
    
    def _get_chunk_offset(self, offset: int) -> int:
        """Calculate offset within chunk"""
        return offset % self.CHUNK_SIZE
    
    def _contact_master(self, operation: str, **kwargs) -> Dict:
        """Contact master server with given operation and parameters"""
        try:
            # In real implementation, this would use gRPC
            # Here we simulate the master server response
            if operation == "get_chunk_locations":
                filename = kwargs.get("filename")
                chunk_index = kwargs.get("chunk_index")
                # Simulate master response
                return {
                    "chunk_handle": f"{filename}-chunk-{chunk_index}",
                    "locations": ["chunkserver1:50051", "chunkserver2:50051", "chunkserver3:50051"]
                }
            elif operation == "create_file":
                return {"status": "success"}
            elif operation == "delete_file":
                return {"status": "success"}
            elif operation == "get_append_location":
                return {
                    "chunk_handle": kwargs.get("chunk_handle"),
                    "primary": "chunkserver1:50051",
                    "secondary": ["chunkserver2:50051", "chunkserver3:50051"]
                }
        except Exception as e:
            raise Exception(f"Failed to contact master: {str(e)}")
    
    def _contact_chunkserver(self, server: str, operation: str, **kwargs) -> Dict:
        """Contact chunk server with given operation and parameters"""
        try:
            # In real implementation, this would use gRPC
            # Here we simulate the chunkserver response
            if operation == "read_chunk":
                return {"data": b"sample data"}
            elif operation == "append_chunk":
                return {"status": "success", "offset": kwargs.get("offset", 0)}
        except Exception as e:
            raise Exception(f"Failed to contact chunkserver {server}: {str(e)}")

    def read(self, filename: str, offset: int, length: int) -> bytes:
        """Read length bytes from filename starting at offset"""
        data = bytearray()
        current_offset = offset
        bytes_remaining = length
        
        while bytes_remaining > 0:
            # Calculate chunk index and offset within chunk
            chunk_index = self._get_chunk_index(current_offset)
            chunk_offset = self._get_chunk_offset(current_offset)
            
            # Get chunk locations from master
            chunk_info = self._contact_master(
                "get_chunk_locations",
                filename=filename,
                chunk_index=chunk_index
            )
            
            # Calculate how much to read from this chunk
            bytes_to_read = min(
                bytes_remaining,
                self.CHUNK_SIZE - chunk_offset
            )
            
            # Try each chunk server until successful
            success = False
            for server in chunk_info["locations"]:
                try:
                    chunk_data = self._contact_chunkserver(
                        server,
                        "read_chunk",
                        chunk_handle=chunk_info["chunk_handle"],
                        offset=chunk_offset,
                        length=bytes_to_read
                    )
                    data.extend(chunk_data["data"])
                    success = True
                    break
                except Exception as e:
                    continue
                    
            if not success:
                raise Exception(f"Failed to read chunk {chunk_index}")
                
            current_offset += bytes_to_read
            bytes_remaining -= bytes_to_read
            
        return bytes(data)

    def append(self, filename: str, data: bytes) -> int:
        """Append data to file, return offset where data was appended"""
        # Get last chunk information from master
        chunk_info = self._contact_master(
            "get_chunk_locations",
            filename=filename,
            chunk_index=-1  # Last chunk
        )
        
        # Get primary and secondary locations for append
        append_info = self._contact_master(
            "get_append_location",
            chunk_handle=chunk_info["chunk_handle"]
        )
        
        # Send append request to primary
        try:
            result = self._contact_chunkserver(
                append_info["primary"],
                "append_chunk",
                chunk_handle=chunk_info["chunk_handle"],
                data=data,
                secondary=append_info["secondary"]
            )
            return result["offset"]
        except Exception as e:
            raise Exception(f"Failed to append: {str(e)}")

    def create(self, filename: str) -> bool:
        """Create a new file"""
        try:
            result = self._contact_master("create_file", filename=filename)
            return result["status"] == "success"
        except Exception as e:
            raise Exception(f"Failed to create file: {str(e)}")

    def delete(self, filename: str) -> bool:
        """Delete a file"""
        try:
            result = self._contact_master("delete_file", filename=filename)
            return result["status"] == "success"
        except Exception as e:
            raise Exception(f"Failed to delete file: {str(e)}")