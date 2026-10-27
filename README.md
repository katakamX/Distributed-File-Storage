# Distributed File Storage System

A prototype distributed file-storage system built using **Python, FastAPI, SQLite, and Docker Compose**. It splits files into chunks, stores replicated copies across multiple storage nodes, and provides a web interface for uploading, downloading, and managing files.

## Features Implemented

- **File Upload and Download:** Upload files through a browser and download them later.
- **File Chunking:** Splits files into chunks of up to 1 MB.
- **Distributed Storage:** Stores file chunks across multiple storage nodes.
- **Replication:** Maintains two copies of each chunk across different storage nodes during upload.
- **Metadata Management:** Uses SQLite to store file names, sizes, checksums, and chunk locations.
- **Data Integrity:** Uses SHA-256 checksums to verify chunks and reconstructed files during download.
- **Node Health Monitoring:** Checks storage-node availability and displays online/offline status.
- **Failure-Aware Interface:** Keeps the stored-file list available when an individual storage node is offline.
- **Automatic Replica Repair:** Periodically checks stored chunks and attempts to restore missing replicas on healthy storage nodes.
- **File Management:** Supports listing and deleting stored files.
- **Containerized Deployment:** Runs the coordinator and three storage nodes using Docker Compose.

## Technology Stack

| Component | Technology |
|---|---|
| Backend API | Python, FastAPI |
| Storage Nodes | Python, FastAPI |
| Metadata Database | SQLite |
| Frontend | HTML, CSS, JavaScript |
| Containerization | Docker, Docker Compose |
| Integrity Verification | SHA-256 |

## Architecture

The system consists of a coordinator and three storage nodes.

- **Coordinator:** Handles upload and download requests, manages metadata, checks node health, and coordinates replica repair.
- **Storage Nodes:** Store individual file chunks and their replicas.
- **SQLite Database:** Maintains file metadata and chunk-placement information.
- **Web Interface:** Allows users to upload, download, delete, and view stored files and node status.

### File Upload Workflow

1. The user selects and uploads a file.
2. The coordinator divides the file into chunks of up to 1 MB.
3. Each chunk is assigned a unique identifier and SHA-256 checksum.
4. The coordinator stores two copies of each chunk on different storage nodes.
5. File metadata and chunk locations are saved in SQLite.

### File Download Workflow

1. The user requests a stored file.
2. The coordinator retrieves its metadata and chunk locations.
3. Chunks are fetched from available replicas.
4. SHA-256 checksums are verified.
5. The chunks are reconstructed and returned as the original file.

### Node Failure Handling

The coordinator periodically checks storage-node health. The web interface displays node availability independently of the file list.

The replica-repair process checks stored chunks and attempts to restore the desired replica count using healthy nodes. Repair is best-effort; complete recovery is not guaranteed if all valid copies of a chunk are unavailable.

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- Git (optional)
- A web browser

### Run the Project

Clone the repository or download and extract the project files.

Open a terminal in the directory containing `docker-compose.yml`, then run:

```bash
docker compose up --build -d
```

Open the web interface:

**http://localhost:8000**

### Useful Commands

Check container status:

```bash
docker compose ps
```

View coordinator logs:

```bash
docker compose logs -f coordinator
```

Stop a storage node to test failure handling:

```bash
docker compose stop node1
```

Restart the storage node:

```bash
docker compose start node1
```

Stop the project:

```bash
docker compose down
```

## Current Limitations

- Automatic replica repair is best-effort and does not guarantee recovery when all valid copies are lost.
- Repair scheduling for nodes that return after being offline is not fully implemented.
- Metadata management uses a single SQLite database and does not provide distributed consensus.
- Authentication, user permissions, and file sharing are not implemented.
- Large-file streaming and performance optimization require further work.

## Future Milestones

- [ ] **Reliable Recovery:** Add persistent repair queues, retry policies, and automatic replica restoration when offline nodes return.
- [ ] **Concurrency and Scalability:** Improve concurrent metadata operations and benchmark upload/download performance with different numbers of storage nodes.
- [ ] **Security and Access Control:** Add user authentication, file permissions, and secure file-sharing functionality.

## Project Goal

The goal of this project is to explore distributed storage concepts, including file chunking, replication, metadata management, data integrity, failure detection, and recovery, through a practical, containerized prototype.
