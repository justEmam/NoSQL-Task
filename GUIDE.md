# Persistent Data Store - Distributed Storage System

A persistent, distributed data storage system built with Python and Flask.

## Features

- **Core Operations**: Put, Fetch, Remove, BatchPut
- **Data Safety**: Transaction Log ensures 100% durability
- **Distributed Mode**: Leader/Follower replication with automatic failover
- **Multi-Leader Mode**: Write to any instance
- **Search Capabilities**:
  - Term-based Query (Full-text search)
  - Vector Similarity (Semantic matching)
- **Atomic Operations**: Atomic batch operations with crash recovery

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

### Launch a Single Instance

```bash
python datastore.py 5000
```

### Use the Connector

```python
from connector import DataStoreConnector

conn = DataStoreConnector(host="localhost", port=5000)

# Put a record
conn.put("rec1", "data1")

# Fetch a record
content = conn.fetch("rec1")

# Remove a record
conn.remove("rec1")

# Batch put
conn.batch_put([("r1", "d1"), ("r2", "d2"), ("r3", "d3")])
```

## Distributed Mode

### Launch a 3-Instance Cluster

```bash
# Terminal 1
set DATASTORE_INSTANCE=inst1
python distributed_store.py 5001

# Terminal 2
set DATASTORE_INSTANCE=inst2
python distributed_store.py 5002

# Terminal 3
set DATASTORE_INSTANCE=inst3
python distributed_store.py 5003
```

### Register Instances to Cluster

```python
import requests

# Register inst1 to cluster
requests.post("http://localhost:5001/cluster/register", json={
    "followers": [
        {"instance": "inst2", "addr": "http://localhost:5002"},
        {"instance": "inst3", "addr": "http://localhost:5003"}
    ]
})

# Promote inst1 to leader
requests.post("http://localhost:5001/promote")
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/fetch/<rid>` | GET | Fetch content by record id |
| `/put` | POST | Put record with id and content |
| `/remove/<rid>` | DELETE | Remove a record |
| `/batch_put` | POST | Batch put multiple records |
| `/query?terms=<query>` | GET | Term-based query |
| `/semantic?terms=<query>&limit=<num>` | GET | Semantic query |
| `/status` | GET | Status check |
| `/cluster/info` | GET | Cluster information |

## Executing Tests

```bash
# Execute all tests
python executor.py --tests

# Execute performance measurements
python executor.py --performance

# Execute everything
python executor.py --all
```

## Architecture

### Transaction Log
- All writes go to transaction log first (synchronous)
- Snapshot updated after log entry
- Recovery replays log on startup
- Ensures 100% durability for confirmed writes

### Leader Election
- Simplified consensus-based election
- Ping-based failure detection
- Automatic failover when leader fails

### Search Structures
- **Term Map**: Maps terms to record ids for fast text queries
- **Vectors**: Simple term vector averaging for semantic similarity

## Debug Mode

For testing crash scenarios, use the `simulate_issue` parameter:

```python
conn.put("rec", "data", simulate_issue=True)  # 1% chance of simulated failure
```

## File Structure

```
kvstore/
├── datastore.py          # Simple single-instance service
├── distributed_store.py  # Distributed service with replication
├── connector.py          # Python connector library
├── test_datastore.py     # Basic tests
├── test_distributed.py   # Distributed tests
├── performance.py        # Performance measurements
├── executor.py           # Test executor
├── requirements.txt      # Dependencies
└── GUIDE.md              # This file
```
