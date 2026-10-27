from app.coordinator import digest, CHUNK_SIZE

def test_sha256():
    assert digest(b"hello") == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

def test_chunk_size():
    assert CHUNK_SIZE == 1024 * 1024
