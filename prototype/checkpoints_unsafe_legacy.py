#!/usr/bin/env python3
"""Filesystem proof of concept for immutable full+incremental manifests.
Pure Python key-value semantics, *not* Substreams Go integration.
Includes atomic publication, deletion/prefix deletion, corruption detection,
missing dependencies, randomized equivalence and cold reconstruction.
"""
import argparse, hashlib, json, os, pickle, random, tempfile, time
from pathlib import Path


def sha(data): return hashlib.sha256(data).hexdigest()


def atomic_write(path, data):
    path=Path(path)
    t=path.with_name(path.name+'.temporary')
    with open(t,'wb') as f:
        f.write(data); f.flush(); os.fsync(f.fileno())
    os.replace(t,path)
    # Ensure parent metadata is durably published on POSIX filesystems
    fd=os.open(path.parent,os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


class CheckpointStore:
    def __init__(self,folder,max_chain=16):
        self.path=Path(folder); self.path.mkdir(parents=True,exist_ok=True)
        self.max_chain=max_chain; self.m=None; self.state={}

    def _writeblob(self, payload):
        data=pickle.dumps(payload,protocol=5)
        h=sha(data)
        filename='blob-'+h+'.bin'
        if not (self.path/filename).exists(): atomic_write(self.path/filename,data)
        return {'file':filename,'hash':h,'bytes':len(data)}

    def _publish(self, manifest):
        raw=json.dumps(manifest,sort_keys=True).encode()
        name='manifest-'+sha(raw)+'.json'
        atomic_write(self.path/name,raw)
        atomic_write(self.path/'HEAD',name.encode())
        self.m=manifest

    def full(self,height):
        base=self._writeblob(self.state)
        self._publish({'height':height,'base':base,'deltas':[],'format':1})

    def ops(self,ops,height,force_delta=False):
        # Apply sequential *logical* operations first; then create net changes.
        # Tombstones for affected prefixes + final values for explicitly written
        # keys. Prefix deletes must precede final value sets on reconstruction.
        prefixes=set(); affected=set(); deletes=set()
        before=self.state
        touched=set()
        for op,arg,value in ops:
            if op=='set':
                self.state[arg]=value
                touched.add(arg)
                deletes.discard(arg)
            elif op=='del':
                self.state.pop(arg,None); touched.add(arg); deletes.add(arg)
            elif op=='prefix':
                prefixes.add(arg)
                # On real store, prefix tombstones can avoid materializing all keys.
                keys=[k for k in self.state if k.startswith(arg)]
                for k in keys: del self.state[k]
                # A write earlier in same segment may have been wiped.
            else: raise ValueError(op)
        # Write only final values for touched keys; prefix invalidations first.
        sets={k:self.state[k] for k in touched if k in self.state}
        dels=[k for k in touched if k not in self.state]
        delta={'prefixes':sorted(prefixes),'sets':sets,'dels':dels}
        if self.m is None or (len(self.m['deltas']) >= self.max_chain and not force_delta):
            self.full(height)
        else:
            entry=self._writeblob(delta)
            manifest={'height':height,'base':self.m['base'],'deltas':self.m['deltas']+[entry], 'format':1}
            self._publish(manifest)
        return delta

    def load(self):
        name=(self.path/'HEAD').read_text()
        raw=(self.path/name).read_bytes()
        if name!='manifest-'+sha(raw)+'.json': raise IOError('Manifest checksum mismatch')
        manifest=json.loads(raw)
        if manifest['format']!=1: raise ValueError('Unknown format')
        def read(entry):
            blob=(self.path/entry['file']).read_bytes()
            if sha(blob)!=entry['hash']: raise IOError('Blob checksum mismatch')
            return pickle.loads(blob)
        out=read(manifest['base'])
        for entry in manifest['deltas']:
            d=read(entry)
            for prefix in d['prefixes']:
                for key in [k for k in out if k.startswith(prefix)]: del out[key]
            for key in d['dels']: out.pop(key,None)
            out.update(d['sets'])
        self.state=out; self.m=manifest
        return out


def unit_test():
    rng=random.Random(777)
    with tempfile.TemporaryDirectory() as tmp:
        s=CheckpointStore(tmp,max_chain=9)
        s.state={f'k/{i:05d}':rng.randbytes(40) for i in range(600)}
        s.full(0)
        checks=0
        for n in range(1,91):
            ops=[]
            for _ in range(rng.randrange(3,20)):
                key=f'k/{rng.randrange(700):05d}'
                op=rng.choices(['set','del','prefix'],[80,15,5])[0]
                ops.append((op,key if op!='prefix' else 'k/00', rng.randbytes(40)))
            s.ops(ops,n)
            expected=s.state.copy()
            loaded=CheckpointStore(tmp,max_chain=9).load()
            assert loaded==expected,('restore mismatch',n)
            checks+=1
        # prefix delete and set-after-delete in one segment
        s.ops([('prefix','k/',None),('set','k/00001',b'new'),('del','k/00001',None),('set','k/00001',b'final')],91)
        assert CheckpointStore(tmp).load()==s.state
        checks+=1
        # uncommitted object is ignored: HEAD always resolves last valid manifest
        orphan=s._writeblob({'prefixes':[],'sets':{'x':b'bad'},'dels':[]})
        assert CheckpointStore(tmp).load()==s.state
        checks+=1
        # corrupt a blob and verify fail-closed behavior (no silent corruption)
        path=Path(tmp)/s.m['deltas'][-1]['file']
        with open(path,'ab') as f: f.write(b'corrupt')
        try: CheckpointStore(tmp).load(); raise AssertionError('corruption was not detected')
        except IOError as exc: assert 'checksum mismatch' in str(exc)
        checks+=1
        path.unlink()
        try: CheckpointStore(tmp).load(); raise AssertionError('missing blob was not detected')
        except FileNotFoundError: pass
        checks+=1
    return checks


def benchmark(entries=22000, value_size=1024, updates=5, seed=8088, churn=.001):
    rng=random.Random(seed)
    vals=[rng.randbytes(value_size) for _ in range(entries)]
    baseline_total=0; delta_total=0; t_full=0; t_delta=0; restore_full=0; restore_delta=0
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp); s=CheckpointStore(root/'deltas', max_chain=16)
        s.state={f'k/{i:08d}':vals[i] for i in range(entries)}
        s.full(0)
        for n in range(1,updates+1):
            changed=rng.sample(range(entries),max(1,int(entries*churn)))
            ops=[('set',f'k/{i:08d}',rng.randbytes(value_size)) for i in changed]
            t=time.perf_counter()
            s.ops(ops,n)
            t_delta+=time.perf_counter()-t
            t=time.perf_counter()
            snapshot=pickle.dumps(s.state,protocol=5)
            (root/f'baseline-{n}.bin').write_bytes(snapshot)
            t_full+=time.perf_counter()-t
            baseline_total+=len(snapshot)
        for p in (root/'deltas').glob('blob-*.bin'):
            delta_total+=p.stat().st_size
        t=time.perf_counter()
        restored=CheckpointStore(root/'deltas').load()
        restore_delta=time.perf_counter()-t
        t=time.perf_counter()
        full_snapshot=pickle.loads((root/f'baseline-{updates}.bin').read_bytes())
        restore_full=time.perf_counter()-t
        assert restored==full_snapshot
        return {'state_size_mib':round(entries*value_size/2**20,2),'value_bytes':value_size,
                'entries':entries,'updates':updates,'churn':churn,'changed_keys_per_step':max(1,int(entries*churn)),
                'baseline_incremental_bytes_mib':round(baseline_total/2**20,2),
                'delta_including_initial_base_mib':round(delta_total/2**20,2),
                'delta_only_mib':round((delta_total-s.m['base']['bytes'])/2**20,3),
                'writing_full_s':round(t_full,4),'writing_delta_s':round(t_delta,4),
                'restore_full_s':round(restore_full,4),'restore_delta_s':round(restore_delta,4)}

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--entries',type=int,default=22000)
    parser.add_argument('--value-size',type=int,default=1024)
    parser.add_argument('--updates',type=int,default=5)
    parser.add_argument('--churn',type=float,default=.001)
    args=parser.parse_args()
    summary={'functional_tests_passed':unit_test(),
             'benchmark':benchmark(args.entries,args.value_size,args.updates,churn=args.churn)}
    print(json.dumps(summary,indent=2))
    with open('/mnt/data/substreams_prototype_results.json','w') as f: json.dump(summary,f,indent=2)
