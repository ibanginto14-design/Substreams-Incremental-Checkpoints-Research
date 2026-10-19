"""Safe-ish *local* proof of concept of bounded full+delta snapshots.

No arbitrary-object deserialization. Does NOT implement object-store conditional writes,
concurrent writers, Substreams' binary on-disk format, lease-based GC, or Go KV APIs.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
from typing import Iterable


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def encode(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp.' + str(os.getpid()))
    with tmp.open('wb') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def value(v: bytes) -> str:
    return base64.b64encode(v).decode('ascii')


def unvalue(v: str) -> bytes:
    return base64.b64decode(v, validate=True)


class Snapshots:
    """Immutable content-addressed blobs and manifests, mutable HEAD.

    Prefix tombstones are applied before explicit final key writes/deletes.
    The authoritative reference state is updated by applying operations in order.
    """

    def __init__(self, path: str | Path, *, max_chain: int = 8):
        assert max_chain >= 1
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.max_chain = max_chain
        self.state: dict[str, bytes] = {}
        self.manifest: dict | None = None

    def _store(self, kind: str, payload: dict) -> dict:
        raw = encode(payload)
        name = f'{kind}-{sha(raw)}.json'
        p = self.path / name
        if not p.exists():
            atomic_write(p, raw)
        return {'name': name, 'sha256': sha(raw), 'bytes': len(raw)}

    def _read(self, desc: dict) -> dict:
        name = desc['name']
        if not name.endswith('.json') or '/' in name or '\\' in name:
            raise ValueError('invalid blob name')
        raw = (self.path / name).read_bytes()
        if sha(raw) != desc['sha256'] or len(raw) != desc['bytes']:
            raise IOError('blob checksum mismatch')
        return json.loads(raw)

    def _publish(self, obj: dict) -> None:
        desc = self._store('manifest', obj)
        atomic_write(self.path / 'HEAD', encode(desc))
        self.manifest = obj

    def full(self, block: int) -> None:
        body = {'kind': 'base', 'values': {k: value(v) for k, v in self.state.items()}}
        base = self._store('blob', body)
        self._publish({'version': 1, 'height': block, 'base': base, 'deltas': []})

    def merge(self, ops: Iterable[tuple[str, str, bytes | None]], block: int, *, force_full=False) -> None:
        prefixes: set[str] = set()
        touched: set[str] = set()
        for op, key, payload in ops:
            if op == 'set':
                assert payload is not None
                self.state[key] = payload
                touched.add(key)
            elif op == 'del':
                self.state.pop(key, None)
                touched.add(key)
            elif op == 'prefix':
                prefixes.add(key)
                for k in [k for k in self.state if k.startswith(key)]:
                    del self.state[k]
            else:
                raise ValueError(f'unsupported op: {op}')
        if self.manifest is None or force_full or len(self.manifest['deltas']) >= self.max_chain:
            self.full(block)
            return
        delta = {'kind': 'delta', 'prefixes': sorted(prefixes),
                 'deletes': sorted(k for k in touched if k not in self.state),
                 'sets': {k: value(self.state[k]) for k in sorted(touched) if k in self.state}}
        desc = self._store('blob', delta)
        self._publish({'version': 1, 'height': block, 'base': self.manifest['base'],
                       'deltas': self.manifest['deltas'] + [desc]})

    def restore(self) -> dict[str, bytes]:
        head = json.loads((self.path / 'HEAD').read_bytes())
        manifest = self._read(head)
        if manifest['version'] != 1 or len(manifest['deltas']) > self.max_chain:
            raise ValueError('unsupported version or unbounded chain')
        base = self._read(manifest['base'])
        if base['kind'] != 'base':
            raise ValueError('invalid base')
        output = {k: unvalue(v) for k, v in base['values'].items()}
        for descriptor in manifest['deltas']:
            delta = self._read(descriptor)
            if delta['kind'] != 'delta':
                raise ValueError('invalid delta')
            for prefix in delta['prefixes']:
                for k in [k for k in output if k.startswith(prefix)]:
                    del output[k]
            for k in delta['deletes']:
                output.pop(k, None)
            for k, v in delta['sets'].items():
                output[k] = unvalue(v)
        self.state = output
        self.manifest = manifest
        return output
