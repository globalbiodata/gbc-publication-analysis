# utils/__init__.py

import os
import gzip
import json
import hashlib

from typing import Optional

default_shard_count = 128

def shard_key(article_id: str, shards: int = default_shard_count) -> int:
    """
    Return the shard key (0 to shards-1) for the given article_id string.
    Uses MD5 hash to ensure even distribution.

    Args:
        article_id (str): The article identifier (e.g., PMC ID).
        shards (int): The total number of shards (default: 128).

    Returns:
        int: The shard key for the given article_id.
    """
    return int(hashlib.md5(article_id.encode()).hexdigest(), 16) % shards

def shard_path(k: int, basepath: str = '', shards: int = default_shard_count) -> str:
    """
    Return the file path for shard key `k` under `basepath`.

    Args:
        k (int): The shard key.
        basepath (str): The base path where the shard file is located.
        shards (int): The total number of shards (default: 128).

    Returns:
        str: The file path for the shard key `k`.
    """
    width = max(2, len(str(max(1, shards) - 1)))
    return os.path.join(basepath, f"metadata_shard_{k:0{width}d}.jsonl.gz")

# Optional in-module cache so repeated lookups in the same shard are fast
_shard_cache = {}
def get_article_metadata(article_id: str, basepath: str = '', shards: int = default_shard_count) -> Optional[dict]:
    """
    Return the metadata dict for `article_id` from sharded JSONL.gz files under `basepath`.
    Expects lines like: {"pmc_id": "...", "meta": {...}}.

    Args:
        article_id (str): The article identifier (e.g., PMC ID).
        basepath (str): The base path where the shard files are located.
        shards (int): The total number of shards (default: 128).

    Returns:
        Optional[dict]: The metadata dictionary for the article_id, or None if not found.
    """
    global _shard_cache
    k = shard_key(article_id, shards)
    if k not in _shard_cache:
        shard_file = shard_path(k, basepath=basepath, shards=shards)
        shard_map = {}
        if os.path.exists(shard_file):
            with gzip.open(shard_file, 'rt', encoding='utf-8') as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                        pid = rec.get('id')
                        if pid is not None:
                            shard_map[str(pid)] = rec.get('meta') or rec
                    except Exception:
                        # swallow bad lines but keep going; optionally log if you want
                        pass

        _shard_cache = {} # Reset the cache for this shard - only hold 1 shard at a time in memory
        _shard_cache[k] = shard_map
    return _shard_cache[k].get(str(article_id))

def sort_ids_by_shard(ids_iterable: list, shards: int = default_shard_count) -> list:
    """Return IDs sorted so that those sharing a shard are contiguous.

    Args:
        ids_iterable (list): List of article IDs to sort.
        shards (int): The total number of shards (default: 128).

    Returns:
        list: The list of article IDs, sorted by their shard key.
    """
    return sorted(ids_iterable, key=lambda _id: shard_key(str(_id), shards))