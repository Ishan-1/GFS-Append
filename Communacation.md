Master Server Response Structure:

For CREATE and APPEND:
json
Copy code
{
    "Status": "SUCCESS",
    "Chunks": [
        {
            "Chunk_ID": 1,
            "Chunkservers": [["localhost", 5001], ["localhost", 5002], ["localhost", 5003]]
        },
        ...
    ],
    "Replicas": 3
}
For READ:
json
Copy code
{
    "Status": "SUCCESS",
    "Chunks": [
        {
            "Chunk_ID": 1,
            "Chunkservers": [["localhost", 5001], ["localhost", 5002], ["localhost", 5003]]
        },
        ...
    ]
}
For DELETE:
json
Copy code
{
    "Status": "SUCCESS",
    "Chunks": [
        {
            "Chunk_ID": 1,
            "Chunkservers": [["localhost", 5001], ["localhost", 5002], ["localhost", 5003]]
        },
        ...
    ]
}
