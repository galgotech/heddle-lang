import hashlib
import polars as pl

def hash_sha256(s):
    return hashlib.sha256(s.encode()).hexdigest()

MOCK_MODULES = {
    "security": {
        "hash": lambda x: hash_sha256(str(x)),
        "anony": lambda x: "anonymous"
    },
    "math": {
        "add": lambda x, y: x + y
    },
    "polars": {
        "from_records": lambda x: pl.from_records(x)
    },
    "data": {},
    "err": {},
    "input": {},
    "http": {}
}

def get_mock_module(alias):
    return MOCK_MODULES.get(alias)