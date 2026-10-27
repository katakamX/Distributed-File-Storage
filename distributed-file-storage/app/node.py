import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

app = FastAPI(title="Storage Node")
ROOT = Path(os.getenv("STORAGE_DIR", "./node-data"))
ROOT.mkdir(parents=True, exist_ok=True)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.put("/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    if not chunk_id.isalnum():
        raise HTTPException(400, "Invalid chunk ID")
    data = await request.body()
    temp = ROOT / (chunk_id + ".tmp")
    temp.write_bytes(data)
    temp.replace(ROOT / chunk_id)
    return {"stored": chunk_id, "bytes": len(data)}

@app.get("/chunks/{chunk_id}")
def get_chunk(chunk_id: str):
    if not chunk_id.isalnum():
        raise HTTPException(400, "Invalid chunk ID")
    path = ROOT / chunk_id
    if not path.is_file():
        raise HTTPException(404, "Chunk missing")
    return Response(path.read_bytes(), media_type="application/octet-stream")

@app.delete("/chunks/{chunk_id}")
def delete_chunk(chunk_id: str):
    if not chunk_id.isalnum():
        raise HTTPException(400, "Invalid chunk ID")
    (ROOT / chunk_id).unlink(missing_ok=True)
    return {"deleted": chunk_id}
