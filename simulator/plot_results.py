#!/usr/bin/env python3
"""Generate illustrative plots from already saved results, never compute new measurements."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT=Path(__file__).resolve().parents[1]
figdir=ROOT/'figures'; figdir.mkdir(exist_ok=True)

def go_plot():
    d=json.loads((ROOT/'results'/'local_go_96_summary.json').read_text())['cases']
    x=np.array([0.1,1,10,50,90]); y=[v['median_speedup'] for v in d]
    fig,ax=plt.subplots(figsize=(8.7,4.4))
    ax.plot(x,y,marker='o',label='Delta write / full write speedup')
    ax.axhline(1,linestyle='--',linewidth=1,label='No improvement (1x)')
    ax.set_xscale('log');ax.set_xticks(x);ax.set_xticklabels([f'{v:g}%' for v in x]); ax.set_ylim(bottom=0)
    ax.set_title('Local Go benchmark: 96 MiB store, median of 3 runs')
    ax.set_xlabel('Net key change fraction');ax.set_ylabel('Measured full-write time / incremental-write time')
    ax.legend();fig.tight_layout();fig.savefig(figdir/'go_local_write_speedup.png',dpi=170);plt.close(fig)

def sim_plot():
    s=json.loads((ROOT/'results'/'extended_summary.json').read_text())['scenarios']
    modes=['fixed','adaptive','guarded','noisy_guarded']
    labels=['Fixed','Threshold','Model-aware (optimistic)','Noisy cost model']
    speedups=[s['all'][m]['median'] for m in modes]
    regress=[s['all'][m]['regression_pct'] for m in modes]
    fig,ax=plt.subplots(figsize=(8.8,4.2))
    xx=np.arange(4)
    ax.bar(xx,speedups)
    ax.set_xticks(xx,labels,rotation=12,ha='right');ax.set_ylabel('Median modeled speedup (x)')
    ax.set_title('Modeled outcomes; synthetic scenarios are not production samples')
    for i,v in enumerate(speedups):ax.text(i,v+.04,f'{v:.2f}x',ha='center',fontsize=9)
    fig.tight_layout();fig.savefig(figdir/'modeled_median_speedup.png',dpi=170);plt.close(fig)
    fig,ax=plt.subplots(figsize=(8.8,4.2))
    ax.bar(xx,regress)
    ax.set_xticks(xx,labels,rotation=12,ha='right');ax.set_ylabel('Model configurations slower than baseline (%)')
    ax.set_title('Adverse outcomes: fixed 19.4%; noisy guard 4.3%')
    for i,v in enumerate(regress):ax.text(i,v+.45,f'{v:.1f}%',ha='center',fontsize=9)
    fig.tight_layout();fig.savefig(figdir/'modeled_regressions.png',dpi=170);plt.close(fig)

if __name__=='__main__':
    go_plot();sim_plot()
