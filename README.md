# GFS-Append


### Planing Day-1
![](assets/1.jpeg)
![](assets/2.jpeg)


### Chunkserver Progress
- Made class for Chunk
- Made Chunk Directory to store metadata 
- Made a common message structure that works with JSON
- Completed handling of connections
- Completed once append for chunkserver



### BUGs 🐛
-  


| tpoic          | Description           | Status   |
|------------------|-----------------------|----------|
| CREATE    | when we create next chunk_no is not start with 1 it start with what left| Resolved |
| Overall  | when chunks reguister they are not sending data properly means i was unable to delete the data present from earlier session | Pending  |
| test  | test for more than 3 chunkservers  all 4 operations | Pending  |
|   | Problem with self.next_Chunk_Number when we want to appent it its mayu be reset id we have done a create befoe it  | Pending  |
|   | in loopp it comes in master  when we swith chunkserver off  Unknown message type from chunkserver 2: None | Pending  |


