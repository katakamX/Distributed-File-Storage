# Distributed File Storage — Runnable Prototype

## Requirements
Docker Desktop (with WSL integration enabled) or Docker Engine with Compose.

## Run
```bash
docker compose up --build -d
```
Open http://localhost:8000 to upload, download, and delete files.

## Test
```bash
curl http://localhost:8000/health
curl http://localhost:8000/nodes
curl -F 'file=@README.md' http://localhost:8000/files
```

## Failure demonstration
1. Upload a file in the browser.
2. Run `docker compose stop node1`.
3. Download the file again. Because every chunk has two replicas on distinct nodes, the download should still work.
4. Restart with `docker compose start node1`.

**Important:** This is a teaching prototype, not production storage. It supports chunking, metadata, per-chunk SHA-256 verification, two-way replication, fallback reads, concurrency via FastAPI, round-robin placement, and in-process metadata caching. It does NOT implement automatic re-replication after failure, quorum writes, distributed consensus, authentication, persistent shared cache, streaming uploads, or cross-node repair. A write failure can leave an orphaned chunk on a node; manual cleanup may be required. Avoid storing sensitive data. Replication factor is 2, so simultaneous loss of two nodes can lose data. SQLite and the coordinator are single points of failure. Do not run `docker compose down -v` unless you want to erase all stored data.
