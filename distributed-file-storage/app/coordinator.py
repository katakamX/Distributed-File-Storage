
import hashlib
import io
import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI(title="Distributed File Storage")

NODES = [
    node.strip()
    for node in os.getenv(
        "NODES",
        "http://localhost:8001,http://localhost:8002,http://localhost:8003"
    ).split(",")
    if node.strip()
]

DB = os.getenv("DATABASE", "metadata.db")
CHUNK_SIZE = 1024 * 1024
REPLICAS = min(2, len(NODES))
LOCK = threading.RLock()
CACHE = {}


def connect():
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB, timeout=30)
    db.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            name TEXT,
            size INTEGER,
            sha256 TEXT,
            chunks TEXT
        )
    """)
    db.commit()
    return db


def digest(data):
    return hashlib.sha256(data).hexdigest()


@app.get("/health")
def health():
    return {"status": "ok", "nodes": len(NODES)}


@app.get("/nodes")
def nodes():
    results = []

    # Check every node independently so one failed node
    # does not prevent the others from being checked.
    for node in NODES:
        healthy = False

        try:
            with httpx.Client(timeout=1.5) as client:
                response = client.get(node + "/health")
                healthy = response.status_code == 200
        except httpx.HTTPError:
            healthy = False

        results.append({
            "url": node,
            "healthy": healthy
        })

    return results


@app.post("/files")
def upload(file: UploadFile = File(...)):
    if not NODES or REPLICAS == 0:
        raise HTTPException(503, "No storage nodes configured")

    file_id = uuid.uuid4().hex
    chunks = []
    whole_hash = hashlib.sha256()
    total = 0

    try:
        with httpx.Client(timeout=30) as client:
            index = 0

            while True:
                data = file.file.read(CHUNK_SIZE)

                if not data:
                    break

                total += len(data)
                whole_hash.update(data)

                chunk_id = uuid.uuid4().hex
                locations = []

                # Rotate node order between chunks.
                for offset in range(len(NODES)):
                    node = NODES[(index + offset) % len(NODES)]

                    try:
                        response = client.put(
                            node + "/chunks/" + chunk_id,
                            content=data
                        )
                        response.raise_for_status()
                        locations.append(node)

                        if len(locations) == REPLICAS:
                            break

                    except httpx.HTTPError:
                        continue

                if len(locations) != REPLICAS:
                    raise HTTPException(
                        503,
                        "Not enough healthy nodes to replicate file"
                    )

                chunks.append({
                    "id": chunk_id,
                    "size": len(data),
                    "sha256": digest(data),
                    "nodes": locations
                })

                index += 1

        record = {
            "id": file_id,
            "name": file.filename or "unnamed",
            "size": total,
            "sha256": whole_hash.hexdigest(),
            "chunks": chunks
        }

        with LOCK:
            with connect() as db:
                db.execute(
                    "INSERT INTO files VALUES (?, ?, ?, ?, ?)",
                    (
                        file_id,
                        record["name"],
                        total,
                        record["sha256"],
                        json.dumps(chunks)
                    )
                )

            CACHE[file_id] = record

        return {
            "id": file_id,
            "name": record["name"],
            "size": total,
            "chunks": len(chunks),
            "replicas": REPLICAS
        }

    except Exception:
        # Best-effort cleanup if an upload fails.
        with httpx.Client(timeout=3) as client:
            for chunk in chunks:
                for node in chunk["nodes"]:
                    try:
                        client.delete(
                            node + "/chunks/" + chunk["id"]
                        )
                    except httpx.HTTPError:
                        pass
        raise


def lookup(file_id):
    with LOCK:
        if file_id in CACHE:
            return CACHE[file_id]

        with connect() as db:
            row = db.execute(
                "SELECT id, name, size, sha256, chunks "
                "FROM files WHERE id = ?",
                (file_id,)
            ).fetchone()

        if row is None:
            raise HTTPException(404, "File not found")

        record = {
            "id": row[0],
            "name": row[1],
            "size": row[2],
            "sha256": row[3],
            "chunks": json.loads(row[4])
        }

        CACHE[file_id] = record
        return record


@app.get("/files")
def list_files():
    with LOCK:
        with connect() as db:
            rows = db.execute(
                "SELECT id, name, size FROM files ORDER BY rowid DESC"
            ).fetchall()

    return [
        {"id": row[0], "name": row[1], "size": row[2]}
        for row in rows
    ]


@app.get("/files/{file_id}")
def metadata(file_id: str):
    return lookup(file_id)


@app.get("/files/{file_id}/download")
def download(file_id: str):
    record = lookup(file_id)
    output = io.BytesIO()
    whole_hash = hashlib.sha256()

    with httpx.Client(timeout=15) as client:
        for chunk in record["chunks"]:
            data = None

            # Try every replica until a valid copy is found.
            for node in chunk["nodes"]:
                try:
                    response = client.get(
                        node + "/chunks/" + chunk["id"]
                    )
                    response.raise_for_status()

                    candidate = response.content

                    if digest(candidate) == chunk["sha256"]:
                        data = candidate
                        break

                except httpx.HTTPError:
                    continue

            if data is None:
                raise HTTPException(
                    503,
                    "Chunk unavailable or corrupt: " + chunk["id"]
                )

            output.write(data)
            whole_hash.update(data)

    if whole_hash.hexdigest() != record["sha256"]:
        raise HTTPException(503, "File integrity check failed")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition":
                "attachment; filename*=UTF-8''" + quote(record["name"])
        }
    )


@app.delete("/files/{file_id}")
def delete(file_id: str):
    record = lookup(file_id)

    with LOCK:
        with connect() as db:
            db.execute("DELETE FROM files WHERE id = ?", (file_id,))

        CACHE.pop(file_id, None)

    # Try to delete every replica, including on offline nodes.
    for chunk in record["chunks"]:
        for node in chunk["nodes"]:
            try:
                with httpx.Client(timeout=3) as client:
                    client.delete(node + "/chunks/" + chunk["id"])
            except httpx.HTTPError:
                pass

    return {"deleted": file_id}


@app.get("/", response_class=HTMLResponse)
def home():
    return """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Distributed File Storage</title>
<style>
body {
    font-family: system-ui, sans-serif;
    background: #f3f6fb;
    color: #172033;
    max-width: 850px;
    margin: 30px auto;
    padding: 20px;
}
main {
    background: white;
    border-radius: 16px;
    padding: 32px;
    box-shadow: 0 8px 30px #0001;
}
button {
    background: #2563eb;
    color: white;
    border: 0;
    padding: 10px 15px;
    border-radius: 7px;
    cursor: pointer;
    margin-left: 8px;
}
button.danger {
    background: #dc2626;
}
li {
    padding: 12px;
    border-bottom: 1px solid #ddd;
    display: flex;
    align-items: center;
    gap: 12px;
    justify-content: space-between;
    flex-wrap: wrap;
}
a {
    color: #2563eb;
}
#message {
    overflow-wrap: anywhere;
}
</style>
</head>
<body>
<main>
<h1>Distributed File Storage</h1>
<p>Upload files across three replicated storage nodes.</p>

<input id="file" type="file">
<button id="uploadButton" onclick="upload()">Upload</button>
<p id="message" role="status"></p>

<h2>Storage nodes</h2>
<div id="nodes">Checking storage nodes...</div>

<h2>Stored files</h2>
<ul id="files">Loading files...</ul>
</main>

<script>
async function loadFiles() {
    const ul = document.getElementById('files');

    try {
        const response = await fetch('/files');

        if (!response.ok) {
            throw new Error('HTTP ' + response.status);
        }

        const files = await response.json();
        ul.replaceChildren();

        if (files.length === 0) {
            ul.textContent = 'No files uploaded yet.';
            return;
        }

        for (const f of files) {
            const li = document.createElement('li');

            const label = document.createElement('span');
            label.textContent = f.name + ' (' + f.size + ' bytes)';

            const a = document.createElement('a');
            a.href = '/files/' + encodeURIComponent(f.id) + '/download';
            a.textContent = 'Download';

            const b = document.createElement('button');
            b.textContent = 'Delete';
            b.className = 'danger';

            b.onclick = async () => {
                b.disabled = true;

                try {
                    const response = await fetch(
                        '/files/' + encodeURIComponent(f.id),
                        { method: 'DELETE' }
                    );

                    if (!response.ok) {
                        throw new Error('Delete failed');
                    }

                    await loadFiles();
                } catch (error) {
                    alert('Could not delete file: ' + error.message);
                    b.disabled = false;
                }
            };

            li.append(label, a, b);
            ul.append(li);
        }
    } catch (error) {
        ul.textContent = 'Could not load files. Please refresh to retry.';
    }
}


async function loadNodes() {
    const box = document.getElementById('nodes');

    try {
        const response = await fetch('/nodes');

        if (!response.ok) {
            throw new Error('HTTP ' + response.status);
        }

        const nodes = await response.json();
        box.replaceChildren();

        for (let i = 0; i < nodes.length; i++) {
            const n = nodes[i];
            const span = document.createElement('span');

            span.textContent =
                n.url + ' ' + (n.healthy ? '🟢 Online' : '🔴 Offline');

            box.append(span);

            if (i < nodes.length - 1) {
                box.append(document.createTextNode(' | '));
            }
        }
    } catch (error) {
        box.textContent = 'Storage node status temporarily unavailable.';
    }
}


async function refresh() {
    // Run independently: a slow node check must not block the file list.
    await Promise.allSettled([
        loadFiles(),
        loadNodes()
    ]);
}


async function upload() {
    const input = document.getElementById('file');
    const button = document.getElementById('uploadButton');
    const message = document.getElementById('message');
    const file = input.files[0];

    if (!file) {
        alert('Choose a file first.');
        return;
    }

    const form = new FormData();
    form.append('file', file);

    button.disabled = true;
    message.textContent = 'Uploading...';

    try {
        const response = await fetch('/files', {
            method: 'POST',
            body: form
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Upload failed');
        }

        message.textContent =
            'Uploaded successfully: ' + data.chunks +
            ' chunk(s), with ' + data.replicas + ' replica(s) each.';

        input.value = '';
        await refresh();
    } catch (error) {
        message.textContent = 'Upload failed: ' + error.message;
    } finally {
        button.disabled = false;
    }
}


// Load the two sections independently on page load.
refresh();
</script>
</body>
</html>"""
