"""Discrete-event simulator invariants, including zero-change and adverse workload cases."""
import unittest
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1] / 'simulator'))
try:
    import numpy as np
    from simulate import gen_workload,generate_trace,upstream_schedule,simulate
except ImportError:
    np=None

@unittest.skipIf(np is None, 'numpy not installed, model tests unavailable')
class SimulatorTests(unittest.TestCase):
    def check_case(self, regime,seed):
        rng=np.random.default_rng(seed)
        _,w=gen_workload(rng,regime,segments=15)
        tape=generate_trace(rng,w,regime)
        arr=upstream_schedule(w,rng)
        baseline=simulate(w,tape,arr,'baseline')
        self.assertGreater(baseline['seconds'],0)
        for mode in ['fixed_delta','adaptive','cost_guarded','noisy_guarded']:
            got=simulate(w,tape,arr,mode)
            self.assertGreater(got['seconds'],0)
            self.assertGreaterEqual(got['bytes_mb'],0)
            self.assertLessEqual(got['peak_chain'],w.k)
        return baseline

    def test_all_regimes(self):
        for i,regime in enumerate(['sparse_io','medium','cpu_bound','massive_change','fast_storage','restart_heavy','prefix_deletes']):
            with self.subTest(regime=regime):
                self.check_case(regime,200+i)

    def test_fully_empty_trace_applies_same_fast_copy(self):
        rng=np.random.default_rng(20261019)
        _,w=gen_workload(rng,'sparse_io',segments=16)
        ch,jitter,merge,restarts,faults,uploads=generate_trace(rng,w,'sparse_io')
        ch[:]=0;restarts[:]=False;faults[:]=False
        trace=(ch,jitter,merge,restarts,faults,uploads)
        arr=upstream_schedule(w,rng)
        baseline=simulate(w,trace,arr,'baseline')
        self.assertEqual(baseline['copy'],16)
        for mode in ['fixed_delta','adaptive','cost_guarded','noisy_guarded']:
            self.assertEqual(simulate(w,trace,arr,mode)['seconds'],baseline['seconds'])

if __name__=='__main__':unittest.main()
