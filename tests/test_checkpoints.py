"""Correctness and fault injection for prototype; run python -m unittest discover -s tests -v."""
import json
import random
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'prototype'))
from checkpoints import Snapshots, atomic_write


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name)
        self.s = Snapshots(self.path, max_chain=7)
        self.s.state = {f'group/{i:04d}': i.to_bytes(4, 'little') for i in range(1200)}
        self.s.full(0)

    def check(self):
        want = self.s.state.copy()
        fresh = Snapshots(self.path, max_chain=7)
        self.assertEqual(fresh.restore(), want)
        self.assertLessEqual(len(fresh.manifest['deltas']), 7)

    def test_randomized_equivalence_1000_commits(self):
        rng = random.Random(20261019)
        for i in range(1, 1001):
            ops = []
            for j in range(rng.randrange(1, 21)):
                k = f'group/{rng.randrange(1500):04d}'
                op = rng.choices(['set', 'del', 'prefix'], [70, 25, 5])[0]
                ops.append((op, 'group/00' if op == 'prefix' else k, rng.randbytes(32)))
            self.s.merge(ops, i)
            self.check()

    def test_prefix_order_and_tombstones(self):
        self.s.merge([('set', 'group/0001', b'A'), ('prefix', 'group/', None),
                      ('set', 'group/0001', b'B'), ('del', 'group/0001', None),
                      ('set', 'group/0001', b'C')], 1)
        self.check()
        self.assertEqual(self.s.state, {'group/0001': b'C'})

    def test_rollback_by_head_repoint(self):
        first = (self.path / 'HEAD').read_bytes()
        previous = self.s.state.copy()
        self.s.merge([('set', 'group/0001', b'next')], 1)
        self.check()
        atomic_write(self.path / 'HEAD', first)
        self.assertEqual(Snapshots(self.path).restore(), previous)

    def test_uncommitted_delta_ignored(self):
        self.s._store('blob', {'kind': 'delta', 'sets': {'evil': 'AAAA'}, 'deletes': [], 'prefixes': []})
        self.check()

    def test_blob_corruption_detected(self):
        self.s.merge([('set', 'group/0001', b'next')], 1)
        head = json.loads((self.path / 'HEAD').read_bytes())
        manifest = self.s._read(head)
        p = self.path / manifest['deltas'][-1]['name']
        with p.open('ab') as f:
            f.write(b'X')
        with self.assertRaises(IOError):
            Snapshots(self.path).restore()

    def test_missing_dependency_detected(self):
        self.s.merge([('set', 'group/0001', b'next')], 1)
        head = json.loads((self.path / 'HEAD').read_bytes())
        manifest = self.s._read(head)
        (self.path / manifest['base']['name']).unlink()
        with self.assertRaises(FileNotFoundError):
            Snapshots(self.path).restore()

    def test_invalid_head_fails_closed(self):
        atomic_write(self.path / 'HEAD', b'{"name":"../../escape","sha256":"x","bytes":2}')
        with self.assertRaises(ValueError):
            Snapshots(self.path).restore()

    def test_compaction_bounds_chain(self):
        for i in range(25):
            self.s.merge([('set', 'group/0001', str(i).encode())], i + 1)
            self.check()

if __name__ == '__main__':
    unittest.main()
