# Reproduction, metrics and validity

## What was measured

### Go local file benchmark (`go-bench/main.go`)

- Deterministic synthetic fixed-width value set (`keyLen=512 B`), 8 update checkpoints, a new full snapshot every 6th incremental boundary.
- Writes and `fsync` actual local files; half of checkpoints serialize the delta first, half the full snapshot first to limit cache/order artifacts.
- Exact equivalence of restored full, delta and adaptive materializations using SHA-256 of the logical state at checkpoints 3 and 8 (both post-mutation).
- Tombstones for some keys. Churn randomized by a pseudorandom permutation with fixed seed per run.
- `FullBytes/DeltaBytes` exclude the common initial base, **include periodic full compaction**.
- `baseline_sec/incremental_sec` measures **write-only**, not blockchain processing. Merge itself is performed once on the shared state; both strategies pay it equally.
- `baseline_sec/scan_delta_sec` adds an actual full-state SHA traversal as an **optimistic lower bound on dirty-key discovery** if the merge cannot supply a mutation journal. It is NOT a full two-version diff.
- Cold restore reads are local, subject to OS page cache. They are not guaranteed cold at disk level. Never call them object-store p99 results.
- An adaptive rule with a fixed churn threshold is a demonstration, not a learned production policy.
- Timings vary with kernel cache/host scheduling, order, concurrency, storage. Small numbers of repeated runs mean ranges are **observed run-to-run variation, not statistical confidence intervals**.
- Compression is gzip level 1 (toy codec), not necessarily Substreams' exact serialization/codec pipeline.

Example from repository root:

```bash
go run ./go-bench -mib 8 -churn 0.001 -out results/smoke.json
python go-bench/run_suite.py --sizes 96 --churn 0.001 0.01 0.1 0.5 0.9 --reps 3 --prefix local_go_96
python go-bench/run_suite.py --sizes 48 --churn 0.001 0.01 0.5 0.9 --reps 2 --compress --prefix local_go_48_gzip
```

### Manifest prototype (`prototype/checkpoints.py`)

The independent local content-addressed JSON+base64 prototype writes a base and bounded sequence of full-state-derived mutations; SHA-256 content addressing, atomic local HEAD publication, fail-closed detection, reorg repoint, prefix tombstones, exact deletes, and compaction. **Does not** implement S3/GCS/Azure consistency, request cancellation, multithreading/lease GC, or production Go. Use `python -m unittest discover -s tests -v`. Never execute untrusted files in the `checkpoints_unsafe_legacy.py` archive (uses Python pickle).

### Synthetic model (`simulator/simulate.py`)

- 2,800 *invented*, equally weighted scenarios across 7 regimes (400 each), not representative samples from production.
- Each scenario has 120 simulated segments; the same simulated arrival times, mutation fractions and failure tapes are reused across policies to permit a paired comparison.
- 2–4 dependency stages, 2–12 worker slots for upstream jobs, sequential checkpointing and shared effective bandwidth fraction.
- Variable entropy/compression/size proxy through write amplification, stochastic reads and retries; zero-mutated segments given the already-available v1.23 empty-copy optimization in **both** baseline and candidates.
- Fixed delta, threshold adaptive, *optimistic oracle-like* cost guard, and cost guard stressed by up to approximately factor-two noise in its estimated write costs.
- Simulation reports speedup `baseline_wall_time/candidate_wall_time`. Negative speedup means regression (factor below 1×), not a negative number.
- Bandwidth sharing is represented by a reduced effective constant rate; **not** per-request packet-level networking, CPU contention, cloud billing, Substreams scheduler, mmap-lock interactions or real WASM.
- Restoration applies simple serial base+deltas cost model and restarts/fault penalties; does not prove correctness (separate prototype does).
- Bootstrap quantiles on this invented workload measure sampling stability **under these assumptions only**; not production confidence intervals.

Run:

```bash
python -m pip install -r requirements.txt
python simulator/simulate.py --n 2800 --segments 120 --seed 20261019 --prefix results/reproduced
```

## Interpretation guardrails

1. 14× quicker *local snapshot writes* is NOT 14× faster Substreams.
2. No proof exists for 2× overall production improvement. Total effect depends on share of time spent serializing/uploading full states.
3. Earlier headline of 5.19× is a median over **specifically favorable synthetic workloads** and must never be detached from that condition.
4. Cost-guarded policy uses known model parameters; the noisy variant stresses imperfections but still is not a calibrated production controller.
5. This repository is a proposal to measure, not a pull request or an upstream patch.
