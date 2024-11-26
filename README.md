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


| topic          | Description           | Status   |
|------------------|-----------------------|----------|
| CREATE    | when we create next chunk_no is not start with 1 it start with what left| Resolved |
| Overall  | when chunks reguister they are not sending data properly means i was unable to delete the data present from earlier session | Resolved  |
| test  | test for more than 3 chunkservers  all 4 operations | Pending  |
|   | Problem with self.next_Chunk_Number when we want to appent it its mayu be reset id we have done a create befoe it  | Resolved  |
|   | in loopp it comes in master  when we swith chunkserver off  Unknown message type from chunkserver 2: None | Resolved  |
|   | In some session if file is created and we try to read it in next session then error | Resolved  |
|   | If file dont exist then dont append | Resolved  |
|   | Append FOr laste files | Pending  |
|   |  Resolved  if chunkserver goes off and comes back then also it works BUT if we delete a file whose a chunk stored in some server is off then also it deletes which should not be done , and sam efor append it append in this case which should definatily not been done | Partially Reasovd |



TODO: Implement lease