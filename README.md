
# Distributed File System (GFS-with exactly one semantic append)

## Project Overview

A distributed file system inspired by Google File System (GFS), featuring a robust master-chunkserver architecture with exactly-once append semantics, scalability, and fault tolerance.

## Working Demo
[Youtube Video](https://youtu.be/AOH6a3y98sw)

## Key Features

- **Distributed Storage**: Scalable and fault-tolerant file system
- **Exactly-Once Append Semantics**: Guarantees precise data appending
- **Master-Chunkserver Architecture**: Centralized metadata management
- **Robust Error Handling**: Comprehensive failure recovery mechanisms
- **Two-Phase Commit (2PC)**: Ensures atomic operations across chunkservers

## System Architecture

### Components

1. **Master Server**
   - Global metadata management
   - Operation coordination
   - Atomicity enforcement

2. **Chunkservers**
   - Distributed storage nodes
   - Chunk storage and management
   - Read and write operations
   - Heartbeat communication

3. **Client Interface**
   - File operation requests
   - Interaction with master and chunkservers

## Core Design Principles

- Horizontal scalability
- Fault tolerance
- Atomic file operations
- Efficient chunk management
- Minimal locking mechanisms

## Exactly-Once Semantic Append

### Challenges and Solutions

1. **Retries During Failures**
   - **Challenge**: Preventing duplicate appends
   - **Solution**: Versioning and idempotency tracking

2. **Partial Appends**
   - **Challenge**: Inconsistent state across chunkservers
   - **Solution**: Two-Phase Commit (2PC) protocol

### Two-Phase Commit (2PC) Workflow

1. **Prepare Phase**
   - Check chunk availability
   - Validate write requirements
   - Allocate new chunks if necessary

2. **Commit Phase**
   - Atomic write across replicas
   - Consistent state maintenance
   - Partial commit support

3. **Abort Mechanism**
   - Rollback partial transactions
   - Clean up pending operations
   - Maintain system consistency

## Failure Scenarios Handling

### Supported Failure Modes

- Network partitions
- Chunkserver crashes
- Master server crashes
- Duplicate client requests

### Robustness Guarantees

- **Atomicity**: All-or-nothing operation commitment
- **Consistency**: Uniform replica state
- **Idempotency**: Prevention of duplicate appends

## Chunkserver Implementation Details

### Responsibilities

- Store and manage file chunks
- Handle read operations
- Manage local chunk directory
- Communicate with master server
- Support transactional append operations

## Advanced Features

### Implemented
- Distributed chunk allocation
- Transactional file appends
- Heartbeat-based chunkserver monitoring
- Dynamic chunkserver registration
- Lease management

### Future Improvements
- Enhanced load balancing
- More sophisticated failure detection
- Advanced caching mechanisms
- Improved transaction timeout handling
- Comprehensive logging and monitoring

## Technical Details

### Technologies
- Python 3.8+
- Socket Programming
- Multithreading
- JSON for Messaging

## Getting Started

### Prerequisites
- Python 3.8 or higher
- Standard Python networking libraries

### Configuration

```python
# Configuration parameters
RETRY_CONNECT_TO_MASTER_TIME = 8  # Reconnection interval
CHUNK_DIRECTORY_PATH = './chunks'  # Default chunk storage
```

### Running a Chunkserver

```bash
python GFS_chunkserver.py <port> <chunk_directory>

# Example
python GFS_chunkserver.py 8001 ./chunkserver1
```


## Contributors
```
Ishan Gupta(Ishan-1)
Harsh Gupta(harsh-gupta10)
```
## Acknowledgments
Inspired by Google File System (GFS) design principles.


