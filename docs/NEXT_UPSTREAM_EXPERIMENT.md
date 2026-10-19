# Proposed native integration experiment (required before claiming a Substreams performance win)

**Goal:** establish whether the checkpoint scheme still saves meaningful wall-clock time after paying the cost of capturing the merge journal, preserving cold-restore guarantees and honoring existing snapshot filenames/semantics.

## Benchmark matrix

For the **same** source blocks and module hash, compare upstream v1.25+ FullKV vs an opt-in delta implementation:

- State size targets: 256 MiB / 1 GiB / 3 GiB, constrained by memory, tiers and available storage. A 4 GiB example is *not* a supported default.
- Dirty data ratios: 0%, 0.01%, 0.1%, 1%, 10%, 50%, 95%. Ensure many prefix-deletes and repeated writes to the same key.
- Memory & mmap backends; network object store, local FS with cold-cache isolation, and one deliberately slow/throttled object storage profile.
- Compression modes and samples with high vs low entropy; record **compressed effective** delta bytes, not just modified key count.
- Store update policies (set, add/min/max and other supported policies), bulk DeletePrefix, sequential merging and reorgs; compare exact reconstructed content/digest to baseline.
- Crash/cancel at upload, manifest publication, restart, prune and GC; concurrent readers/writers; old version reader compatibility.
- End-to-end case matrix: state-heavy module where full snapshot saving actually dominates; map-only/CPU-bound module as negative control; zero-partial module that already uses fast copy as negative control.
- Report p50/p95/p99 cold restore, wall time, block/s, CPU time, mmap snapshot-lock hold time, bytes read/written, object GET/PUT counts, memory peak, reorg/retry rate and cloud cost.

Only then should the post be upgraded from 'independent prototype proposal' to 'native Substreams optimization.'
