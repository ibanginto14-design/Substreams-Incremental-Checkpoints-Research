#!/usr/bin/env python3
"""Independent reproducible *synthetic* performance model; NOT a Substreams server benchmark.
Designed to compare full-state saves with bounded incremental saves, including the
v1.23 empty-partial optimization. Python 3.10+, numpy and pandas.
"""
import argparse, heapq, json, math, os
from dataclasses import dataclass
import numpy as np
import pandas as pd

@dataclass
class Workload:
    state_mb: float
    bw_mb_s: float
    cpu_sec_mb: float
    delta_cpu_sec_mb: float
    merge_s: float
    head_latency_s: float
    workers: int
    upstream_mean_s: float
    segments: int
    k: int
    churn: float
    sparsity: float
    write_amp: float
    extra_scan_sec_mb: float
    restore_prob: float
    corruption_prob: float
    upload_hazard: float
    retry_base_s: float
    bw_share: float
    read_bw_factor: float
    n_layers: int
    prefix_delete_fraction: float


def gen_workload(rng, regime=None, segments=120):
    regimes = ['sparse_io', 'medium', 'cpu_bound', 'massive_change', 'fast_storage', 'restart_heavy', 'prefix_deletes']
    regime = regime or rng.choice(regimes)
    state = float(np.exp(rng.uniform(np.log(96), np.log(3584))))
    bandwidth = float(np.exp(rng.uniform(np.log(110), np.log(1200))))
    churn = {'sparse_io':(0.0001,0.003),'medium':(0.004,0.08),
             'cpu_bound':(0.0001,0.01),'massive_change':(0.3,0.95),
             'fast_storage':(0.0001,0.015),'restart_heavy':(0.0002,0.03),
             'prefix_deletes':(0.0002,0.025)}[regime]
    churn = float(np.exp(rng.uniform(np.log(churn[0]), np.log(churn[1]))))
    merge = float(rng.uniform(0.02, 0.3))
    compute = float(np.exp(rng.uniform(np.log(.15),np.log(7))))
    if regime == 'cpu_bound': compute *= rng.uniform(10,45); merge *= 10
    if regime == 'fast_storage': bandwidth *= 3.5
    if regime == 'sparse_io': bandwidth *= 0.6
    restart_p = 0.002 if regime != 'restart_heavy' else 0.075
    return regime, Workload(
        state_mb=state, bw_mb_s=bandwidth, cpu_sec_mb=float(rng.uniform(.0004,.007)),
        delta_cpu_sec_mb=float(rng.uniform(.002,.06)), merge_s=merge,
        head_latency_s=float(rng.uniform(.015,.14)), workers=int(rng.choice([2,4,8,12])),
        upstream_mean_s=compute, segments=segments,
        k=int(rng.choice([8,16,32,64,96])), churn=churn,
        sparsity=float(rng.uniform(.01,.32)),
        write_amp=float(rng.uniform(1.04,2.25)),
        extra_scan_sec_mb=float(rng.choice([0,0,0,0,0,0.001,0.005])),
        restore_prob=restart_p, corruption_prob=.0002,
        upload_hazard=float(rng.uniform(.00004,.002)), retry_base_s=float(rng.uniform(.05,.3)),
        bw_share=float(rng.uniform(.3,1)),read_bw_factor=float(rng.uniform(.5,1.5)),
        n_layers=int(rng.choice([2,3,4])),
        prefix_delete_fraction=float(rng.uniform(.3,1)) if regime=='prefix_deletes' else 0)


def generate_trace(rng, w, regime):
    n = w.segments
    # Shared traces to give paired comparisons with precisely the same source load
    on = rng.random(n) > w.sparsity
    raw = np.exp(rng.normal(np.log(w.churn),0.75,n))
    churns = np.minimum(1., raw) * on
    if regime=='prefix_deletes':
        # A prefix-delete can invalidate many entries despite few explicit update ops
        burst = (rng.random(n)<.09) & on
        churns[burst] = np.maximum(churns[burst], rng.uniform(.25,.95,size=burst.sum()))
    # independent failure and restart tapes; same for both policies
    jitter = rng.lognormal(0., .28, size=n)
    merge_jitter = rng.lognormal(0., .3, size=n)
    restarts = rng.random(n) < w.restore_prob
    faults = rng.random(n) < w.corruption_prob
    upload_uniform = rng.random((n,4))
    return churns, jitter, merge_jitter, restarts, faults, upload_uniform


def upstream_schedule(w, rng):
    """Event-driven dependent CPU pipeline, limited shared worker pool.
    n segments * n_layers jobs. Later stages unlock only after their parent
    completes, with no global stage barriers; all runs share the same trace.
    """
    n=w.segments
    ready=[(i,0) for i in range(n)]
    heapq.heapify(ready)
    running=[]
    completed=np.zeros(n)
    free=w.workers
    now=0.
    remaining=n*w.n_layers
    while remaining:
        while free and ready:
            i,layer=heapq.heappop(ready)
            duration=rng.lognormal(math.log(w.upstream_mean_s),.5)
            heapq.heappush(running,(now+duration,i,layer))
            free-=1
        now,i,layer=heapq.heappop(running)
        free+=1
        remaining-=1
        if layer+1 < w.n_layers:
            heapq.heappush(ready,(i,layer+1))
        else:
            completed[i]=now
    return completed

def restore_cost(w, deltas_mb, encoded_base_mb, is_delta):
    """Cold-read one base plus deltas; no cache, sequential apply and per-object latency."""
    r_bw=max(1.,w.bw_mb_s*w.bw_share*w.read_bw_factor)
    if not is_delta:
        return w.head_latency_s + encoded_base_mb/r_bw + encoded_base_mb*w.cpu_sec_mb*.55
    d=sum(deltas_mb)
    return ((len(deltas_mb)+1)*w.head_latency_s + (encoded_base_mb+d)/r_bw
            + encoded_base_mb*w.cpu_sec_mb*.55 + d*w.delta_cpu_sec_mb*.8
            + .002*len(deltas_mb))


def simulate(w, trace, arrivals, mode):
    churns,jitter,merge_jitter,restarts,faults,upload_uniform=trace
    full=w.state_mb
    finished=0.; queued_max=0; queue_sum=0.;
    bytes_written=0.; writes=0; full_ct=0; delta_ct=0; copy_ct=0; retries=0
    recover_s=0.; delta_chain=[]; full_by_last=full
    peak_chain=0; restoration_failures=0
    # External co-tenants reduce BW, modeled as share of a physical bandwidth budget.
    effective_bw=max(1.,w.bw_mb_s*w.bw_share)
    for i,f in enumerate(churns):
        arrival = arrivals[i]
        # an estimate of outstanding ready-to-squash segments, not a separate simulation of queue jobs
        queue_sum += float(finished>arrival)
        queued_max=max(queued_max, int(finished>arrival))
        finished=max(finished,arrival)
        if restarts[i]:
            if mode=='baseline':
                c=restore_cost(w,[],full,False)
            else:
                c=restore_cost(w,delta_chain,full,True)
            finished += c
            recover_s += c
        if faults[i]:
            # corrupted payload is detected by checksum; model fallback re-fetch
            penalty=restore_cost(w,delta_chain if mode!='baseline' else [],full,mode!='baseline')
            finished += penalty
            recover_s += penalty
            restoration_failures += 1
        if f<=0:
            # Existing v1.23 copy-over-empty optimization applies to both modes.
            finished += .035 + w.head_latency_s*.3
            copy_ct += 1
            continue
        change_mb=full*f*w.write_amp
        # write full if threshold exceeded or K bounded; otherwise delta
        if mode=='baseline':
            kind='full'
        elif mode=='fixed_delta':
            kind='full' if (len(delta_chain) >= w.k) else 'delta'
        elif mode in ('adaptive','cost_guarded','noisy_guarded'):
            # finite chain, expanded deltas, predicted extra cold-restore cost
            extra_restore=restore_cost(w,delta_chain+[change_mb],full,True)-restore_cost(w,[],full,False)
            saved=(full-change_mb)/effective_bw
            choose_full=(len(delta_chain)>=w.k or change_mb>full*.43
                         or (w.restore_prob*extra_restore>saved*.75)
                         or (w.extra_scan_sec_mb*full>saved*.9))
            if mode in ('cost_guarded','noisy_guarded'):
                cost_full=w.cpu_sec_mb*full+full/effective_bw
                cost_delta=w.delta_cpu_sec_mb*change_mb+w.extra_scan_sec_mb*full+change_mb/effective_bw
                # A practical guard would estimate these costs from moving averages,
                # not know actual future latencies. This is an OPTIMISTIC oracle bound.
                # A deliberately noisy telemetry policy to measure guard brittleness.
                # Predict each path with up to roughly 2x error. This does NOT use future observations.
                if mode=='noisy_guarded':
                    cost_delta *= math.exp(.70*math.sin(i*1.73+w.churn*1000))
                    cost_full *= math.exp(.70*math.cos(i*.91+w.state_mb/300))
                choose_full = choose_full or cost_delta > .88*cost_full
            kind='full' if choose_full else 'delta'
        else: raise ValueError(mode)
        if kind=='full':
            mb=full
            cpu=w.cpu_sec_mb*full
            full_ct+=1
            delta_chain.clear()
        else:
            mb=change_mb+.04
            cpu=w.delta_cpu_sec_mb*change_mb + w.extra_scan_sec_mb*full
            delta_ct+=1
            delta_chain.append(mb)
        peak_chain=max(peak_chain,len(delta_chain))
        # CPU merge is not accelerated; duplicate unchanged work removed only from persistence
        finished += w.merge_s*merge_jitter[i] + cpu + mb/effective_bw*jitter[i] + w.head_latency_s
        # unsuccessful transfers need another attempt; duration-dependent fault hazard
        nominal=mb/effective_bw*jitter[i]
        fail_p=1.-math.exp(-w.upload_hazard*nominal)
        for r in range(4):
            if upload_uniform[i,r]>=fail_p: break
            finished += .5*nominal + w.retry_base_s*(2**r)
            bytes_written += .5*mb
            retries += 1
        else:
            # cap tail retries; intended to keep simulation bounded, not to conceal failure
            finished += 2.*nominal
            bytes_written+=2*mb
        bytes_written += mb; writes+=1
    return dict(seconds=finished, bytes_mb=bytes_written, writes=writes,
                full=full_ct,delta=delta_ct,copy=copy_ct,retries=retries,
                recovery_seconds=recover_s,peak_chain=peak_chain,corruption_events=restoration_failures,
                queue_busy_fraction=queue_sum/w.segments,queue_busy_peak=queued_max)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--n',type=int,default=2800)
    ap.add_argument('--segments',type=int,default=120)
    ap.add_argument('--seed',type=int,default=20261019)
    ap.add_argument('--prefix',default='results/simulation')
    args=ap.parse_args()
    rng=np.random.default_rng(args.seed)
    rows=[]
    regimes=['sparse_io','medium','cpu_bound','massive_change','fast_storage','restart_heavy','prefix_deletes']
    for j in range(args.n):
        regime=regimes[j%len(regimes)]
        _,w=gen_workload(rng,regime,args.segments)
        trace=generate_trace(rng,w,regime)
        arrivals=upstream_schedule(w,rng)
        a=simulate(w,trace,arrivals,'baseline')
        b=simulate(w,trace,arrivals,'fixed_delta')
        c=simulate(w,trace,arrivals,'adaptive')
        d=simulate(w,trace,arrivals,'cost_guarded')
        e=simulate(w,trace,arrivals,'noisy_guarded')
        row={'case':j,'regime':regime,'state_mb':w.state_mb,'change_frac':w.churn,
             'bw_mb_s':w.bw_mb_s*w.bw_share,'check_every':w.k,'workers':w.workers,
             'restore_prob':w.restore_prob,'scan_overhead_s_mb':w.extra_scan_sec_mb,
             'empty_probability':w.sparsity,'source_layers':w.n_layers,
             'baseline_s':a['seconds'],'fixed_s':b['seconds'],'adaptive_s':c['seconds'],
             'speedup_fixed':a['seconds']/b['seconds'],
             'speedup_adaptive':a['seconds']/c['seconds'],
             'speedup_guarded':a['seconds']/d['seconds'],
             'speedup_noisy_guarded':a['seconds']/e['seconds'],
             'write_reduction_fixed_pct':100*(1-b['bytes_mb']/a['bytes_mb']),
             'write_reduction_adaptive_pct':100*(1-c['bytes_mb']/a['bytes_mb']),
             'write_reduction_guarded_pct':100*(1-d['bytes_mb']/a['bytes_mb']),
             'write_reduction_noisy_guarded_pct':100*(1-e['bytes_mb']/a['bytes_mb']),
             'baseline_written_mb':a['bytes_mb'],'fixed_written_mb':b['bytes_mb'],
             'adaptive_written_mb':c['bytes_mb'],'guarded_written_mb':d['bytes_mb'],
             'fixed_restore_s':b['recovery_seconds'],'adaptive_restore_s':c['recovery_seconds'],'guarded_restore_s':d['recovery_seconds'],
             'baseline_restore_s':a['recovery_seconds'],
             'fixed_peak_chain':b['peak_chain'],'adaptive_peak_chain':c['peak_chain'],'guarded_peak_chain':d['peak_chain'],
             'guarded_full_count':d['full'],'adaptive_full_count':c['full'],'adaptive_delta_count':c['delta'],
             'noisy_guarded_written_mb':e['bytes_mb'],
             'baseline_retries':a['retries'],'fixed_retries':b['retries'],
             'adaptive_retries':c['retries'],
             'fault_events':sum(trace[4]), 'restart_events':sum(trace[3]),
             'baseline_copy_count':a['copy']}
        rows.append(row)
    df=pd.DataFrame(rows)
    out=args.prefix+'_cases.csv'; df.to_csv(out,index=False)
    summary={'seed':args.seed,'cases':len(df),'segments_per_case':args.segments,'scenarios':{}}
    for typ,group in [('all',df)]+list(df.groupby('regime')):
        rec={}
        for mode in ['fixed','adaptive','guarded','noisy_guarded']:
            v=group['speedup_'+mode].to_numpy()
            rec[mode]={'median':round(float(np.median(v)),3),
                       'p05':round(float(np.quantile(v,.05)),3),
                       'p95':round(float(np.quantile(v,.95)),3),
                       'mean':round(float(v.mean()),3),
                       'ge_2x_pct':round(float(np.mean(v>=2.)*100),1),
                       'regression_pct':round(float(np.mean(v<1.)*100),1),
                       'bytes_saved_median_pct':round(float(group['write_reduction_'+mode+'_pct'].median()),1)}
        summary['scenarios'][typ]=rec
    with open(args.prefix+'_summary.json','w') as f: json.dump(summary,f,indent=2,ensure_ascii=False)
    print(json.dumps(summary,indent=2,ensure_ascii=False))
    print('CSV:',out)

if __name__=='__main__': main()
