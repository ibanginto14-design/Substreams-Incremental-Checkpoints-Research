#!/usr/bin/env python3
"""Run independent local Go file benchmarks; collect every individual run."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import statistics
import sys
import time

ROOT=Path(__file__).resolve().parents[1]

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--reps',type=int,default=3)
    ap.add_argument('--sizes',type=int,nargs='+',default=[96])
    ap.add_argument('--churn',type=float,nargs='+',default=[0.001,0.01,0.1,0.5,0.9])
    ap.add_argument('--compress',action='store_true')
    ap.add_argument('--prefix',default='local_go')
    args=ap.parse_args()
    out=ROOT/'results'
    out.mkdir(exist_ok=True)
    path=out/f'{args.prefix}_runs.json'
    exe=out/'kvbench_tmp'
    subprocess.run(['go','build','-o',str(exe),'./go-bench'],cwd=ROOT,check=True)
    records=[]
    start=time.monotonic()
    try:
        for size in args.sizes:
            for churn in args.churn:
                for rep in range(args.reps):
                    temp=out/'go_out_tmp.json'
                    cmd=[str(exe),'-out',str(temp),'-mib',str(size),'-churn',str(churn),'-compress='+str(args.compress).lower()]
                    p=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True)
                    if p.returncode:
                        raise RuntimeError(f'{cmd}\n{p.stderr}')
                    r=json.loads(temp.read_text())[0]
                    r['repeat']=rep+1
                    r['write_speedup_full_over_delta']=r['baseline_sec']/r['incremental_sec']
                    r['write_speedup_full_over_scan_delta']=r['baseline_sec']/r['scan_delta_sec']
                    r['byte_saving_pct']=100*(1-r['delta_bytes']/r['full_bytes'])
                    r['cold_restore_ratio_delta_over_full']=r['delta_restore_sec']/r['baseline_restore_sec']
                    records.append(r)
                    path.write_text(json.dumps({'source':'local standalone Go synthetic checkpoints, not Substreams server', 'machine':platform.platform(),'go':subprocess.run(['go','version'],capture_output=True,text=True).stdout.strip(),'cases':records},indent=2))
                    print(f'{size}MiB churn={churn:g} repeat={rep+1}/{args.reps}: speedup {r["write_speedup_full_over_delta"]:.2f}x; scan-delta {r["write_speedup_full_over_scan_delta"]:.2f}x; verified={r["recovery_correct"]}',flush=True)
    finally:
        if exe.exists(): exe.unlink()
        (out/'go_out_tmp.json').unlink(missing_ok=True)
    grouped={}
    for r in records:
        label=f'{r["mib"]}MiB / churn={r["churn"]} / compress={r["compress"]}'
        grouped.setdefault(label,[]).append(r)
    summary=[]
    for label,rr in grouped.items():
        summary.append({'scenario':label, 'runs':len(rr),
         'median_speedup':statistics.median(v['write_speedup_full_over_delta'] for v in rr),
         'min_speedup':min(v['write_speedup_full_over_delta'] for v in rr),
         'max_speedup':max(v['write_speedup_full_over_delta'] for v in rr),
         'median_scan_speedup':statistics.median(v['write_speedup_full_over_scan_delta'] for v in rr),
         'median_restore_ratio':statistics.median(v['cold_restore_ratio_delta_over_full'] for v in rr),
         'median_byte_saved_pct':statistics.median(v['byte_saving_pct'] for v in rr),
         'verified':all(v['recovery_correct'] for v in rr)})
    (out/f'{args.prefix}_summary.json').write_text(json.dumps({'elapsed_s':time.monotonic()-start,'cases':summary},indent=2))
    print('DONE:',path, flush=True)

if __name__=='__main__': main()
