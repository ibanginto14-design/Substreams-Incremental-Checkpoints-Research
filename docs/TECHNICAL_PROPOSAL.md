# Proposal: bounded incremental FullKV checkpoints for Substreams

**Status: independent research proof of concept; not merged into, or benchmarked against, Substreams itself.**

## Where it applies

Persistent state-heavy Substreams store modules whose full state is large but whose **net changes per squash interval** are small. This proposal does not accelerate block decode, WASM execution, general map-only workloads, or the merge computation.

The currently published `FullKV.Save` obtains `kvImpl.Snapshot()` and uses `marshaller.MarshalStreamSnapshot` for the entire store, then returns a `fileWriter` to upload; the mmap backend's snapshot can gate writes while the upload remains active. `FullKV.Load` streams entries back into the store. Code anchors:

- https://github.com/streamingfast/substreams/blob/develop/storage/store/full_kv.go#L174-L247
- https://github.com/streamingfast/substreams/blob/develop/orchestrator/stage/squash.go
- https://github.com/streamingfast/substreams/releases/tag/v1.23.0
- https://github.com/streamingfast/substreams/releases/tag/v1.25.0

v1.23 introduced server-side copies over **fully empty** partials. Any meaningful comparison must give this optimization to the baseline too. Our proposal covers **nonempty low-churn** intervals. The new Rust Buffa protobuf codec has different scope and does not inherently remove the FullKV persistence problem.

## Representation

```
store/<module-id>/<end-block>.checkpoint -> manifest v1
manifest {
  module_id, module_hash, initial_block, end_block,
  state_format, codec, update_policy_version,
  full_object: { key, length, sha256 },
  ordered_deltas: [{ key, length, sha256, start_block, end_block }],
  checksum
}

Delta {
  final_key_values: [(key, final_value)],
  exact_deletions: [key],
  deleted_prefixes: [prefix],
  operation_semantics_version
}
```

Within one squash interval, apply operations with **actual Substreams update policy semantics**, then derive the final state delta. On replay, apply prefix tombstones before final writes, but guarantee the final canonical state matches the existing sequential merge. Prefix tombstones can delete many keys even when the number of explicit operations is small; measuring 'changed operation count' is insufficient. Canonicalization, ordering, and delete-after-set cases require differential property tests against upstream's implementation.

## Safe publication protocol (object store)

1. Compute a full or delta object from an immutable view of the correct store state.
2. Upload the immutable payload to a versioned content-addressed key; verify digest and size.
3. Write a separate immutable manifest referencing a **fully uploaded** dependency set.
4. Publish or update the canonical checkpoint pointer only by a conditional write (ETag/generation CAS) or an equivalent transaction. Never point to partially uploaded data. Verify both concurrent-writer exclusion and correct range/module identity.
5. Reader pins a single manifest, checks all digests, reconstructs in order, and fails closed on missing/corrupted dependencies. Retry/fallback to a full snapshot **only if its identity and boundary are verified**.
6. Retention/GC must traverse live manifests and in-flight readers, including reorg versions and caches. Unreferenced blobs may be deleted only after a safe grace period. No in-place mutation of referenced objects.

The local proof of concept uses POSIX rename + fsync, **not** cloud CAS; this needs a server implementation.

## Policy (heuristic, not proven optimal)

A first pass enables delta only if all safeguards pass:

- Nonempty delta and low changed **encoded bytes / full encoded bytes**, measured after compression.
- A bounded chain (e.g. <= 8, tuned on tail restore latency, not chosen for publicity).
- Historical moving-average write cost predicts meaningful net benefit, with a pessimistic uncertainty margin.
- Predicted p95/p99 cold restore and number of GETs stay within budget; expected reorg/restart rate considered.
- If dirty-key discovery needs a full scan and is not already tracked during merge, choose full.
- For large `DeletePrefix` or overwrite-heavy workloads, force full if their true effective delta exceeds threshold.
- Preserve today's empty-partial fast copy behavior.

Avoid using the simulator's near-oracle cost guard as an implementable policy. Include noisy-cost and adversarial tests.

## Approximate integration plan — not a trivial patch

1. Add optional mutation journal to merge path (by module/range, ordered, deduplicated at boundary). Must respect all `update policies` and both memory/mmap stores; compare bit-for-bit against full snapshots.
2. Add versioned snapshot metadata/content type; old filenames and existing complete files remain readable.
3. Extend `FullKV.Load`, restore/retry paths, quicksave/resume, squasher and object store wrappers to handle full-or-delta manifests.
4. Implement boundary-safe publication, leases/refcounts/grace periods and GC. Integrate reorg rollback and cancellation behavior.
5. Measure warm/cold writes and restores over real substreams packages on exact same blocks. Compare against v1.25+ with empty-copy and Buffa enabled. Collect wall-clock end-to-end, throughput, CPU, network bytes, requests, total cost, memory and p99 restore latency. Use fixed cluster/machine resources and identical cold-cache profiles.
6. Opt-in/feature flag, dual-write verification for a pilot, incremental rollout with automatic fallback on unsound predictions.

## Evidence ladder

- [x] Standalone Python snapshot algebra; 1,000 randomized interval equivalence checks; corruption/missing data/reorg/atomic-local-head tests.
- [x] Standalone Go 8-boundary **local file** benchmarks at several churn/compressibility rates; each checkpoint includes fsync and exact restore digest; multiple repeated runs. Neither actual Substreams format nor `mmap`/bbolt.
- [x] Synthetic model: 2,800 scenarios × 120 segments, with dependency stages, writes, reads, modeled retries, cold restarts, failure likelihood and policy alternatives; synthetic *assumed* inputs, not production distribution.
- [ ] In-process Substreams Go integration tests and microbenchmarks on `FullKV`.
- [ ] Native Substreams server benchmarks with actual blockchain packages and object-store latencies.
- [ ] Cloud fault injection under real CAS/lease/GC semantics.
- [ ] Controlled production canary.

## Primary failure modes

Unbounded delta chains, wrong prefix/delete semantics, undercounted dirty bytes, full scan overhead, too many GET requests, retention races, reorg invalidation, manifest pointed at missing objects, lock held during cloud upload, hidden CPU cost, compression-dependent reversals, model-to-reality mismatch and p99 latency inflation.
