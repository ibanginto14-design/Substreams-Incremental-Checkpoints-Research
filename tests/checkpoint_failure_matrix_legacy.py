#!/usr/bin/env python3
"""Exercise manifest prototype from prior investigation; NOT Substreams Go."""
import sys,random,tempfile,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1] / 'prototype'))
from checkpoints_unsafe_legacy import CheckpointStore, unit_test, atomic_write

def run():
    total=unit_test()
    details={'existing_prototype_checks':total}
    rng=random.Random(20261019)
    ncheck=0
    with tempfile.TemporaryDirectory() as t:
        s=CheckpointStore(t,max_chain=11)
        s.state={f'a/{i:05}':rng.randbytes(96) for i in range(5000)}
        s.full(0)
        rollback_heads=[]
        # change 1-100 keys per step, prefix deletion impacts up to 5k keys
        for i in range(1,501):
            ops=[]
            for j in range(rng.randrange(1,35)):
                key=f'a/{rng.randrange(5500):05d}'
                op=rng.choices(['set','del','prefix'],weights=[.76,.21,.03])[0]
                ops.append((op,key if op!='prefix' else 'a/00',rng.randbytes(96)))
            s.ops(ops,i)
            r=CheckpointStore(t,max_chain=11).load()
            if r!=s.state:raise AssertionError(f'mismatch after step {i}')
            ncheck+=1
            if i in (111,239,419): rollback_heads.append(((Path(t)/'HEAD').read_bytes(),dict(s.state)))
        # Simulate reorg: atomically repoint HEAD at a previous immutable manifest
        for head,expected in rollback_heads:
            atomic_write(Path(t)/'HEAD',head)
            loaded=CheckpointStore(t,max_chain=11).load()
            assert loaded==expected
            ncheck+=1
        # fail on missing manifest, retained raw checkpoints not accidentally reused
        atomic_write(Path(t)/'HEAD',b'does-not-exist')
        try: CheckpointStore(t,max_chain=11).load();raise AssertionError('bad HEAD was accepted')
        except FileNotFoundError: ncheck+=1
    details.update(extra_checks=ncheck,total_passed=total+ncheck,go_integrated=False)
    return details
if __name__=='__main__':
    r=run();print(json.dumps(r,indent=2));# no implicit overwriting of tracked results
