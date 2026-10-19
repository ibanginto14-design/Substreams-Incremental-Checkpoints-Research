// Verifies actual local persistence costs of full and bounded-incremental KV snapshots.
// Standalone Go benchmark. NOT streamingfast/substreams server code or its binary format.
package main

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

const keyLen = 512
const nSteps = 8
const compactEvery = 6
const magic uint64 = 0x5eedc0de01000002

type Scenario struct {
	MiB      int     `json:"mib"`
	Churn    float64 `json:"churn"`
	Compress bool    `json:"compress"`
	Mode     string  `json:"mode"`
}
type Result struct {
	Scenario
	Keys               int     `json:"keys"`
	BaselineSec        float64 `json:"baseline_sec"`
	IncrementalSec     float64 `json:"incremental_sec"`
	ScanDeltaSec       float64 `json:"scan_delta_sec"`
	AdaptiveSec        float64 `json:"adaptive_sec"`
	FullBytes          int64   `json:"full_bytes"`
	DeltaBytes         int64   `json:"delta_bytes"`
	AdaptiveBytes      int64   `json:"adaptive_bytes"`
	BaselineRestoreSec float64 `json:"baseline_restore_sec"`
	DeltaRestoreSec    float64 `json:"delta_restore_sec"`
	AdaptiveRestoreSec float64 `json:"adaptive_restore_sec"`
	MergeSec           float64 `json:"merge_sec"`
	ScanSec            float64 `json:"scan_sec"`
	FullWrites         int     `json:"full_writes"`
	DeltaWrites        int     `json:"delta_writes"`
	AdaptiveFullWrites int     `json:"adaptive_full_writes"`
	RecoveryCorrect    bool    `json:"recovery_correct"`
	Checkpoints        int     `json:"checkpoints"`
	Notes              string  `json:"notes"`
}

// Simple bounded file writer: fixed-width key index and length; -1 value-length means deletion.
func save(p string, state [][]byte, dirty []int, full bool, compress bool) (int64, float64, error) {
	st := time.Now()
	f, e := os.Create(p)
	if e != nil {
		return 0, 0, e
	}
	b := bufio.NewWriterSize(f, 1<<20)
	var w io.Writer = b
	var gz *gzip.Writer
	if compress {
		gz, e = gzip.NewWriterLevel(b, gzip.BestSpeed)
		if e != nil {
			return 0, 0, e
		}
		w = gz
	}
	count := len(dirty)
	if full {
		count = len(state)
	}
	var hdr [16]byte
	binary.LittleEndian.PutUint64(hdr[0:8], magic)
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(count))
	_, e = w.Write(hdr[:])
	if e != nil {
		return 0, 0, e
	}
	var rec [8]byte
	emit := func(i int) error {
		v := state[i]
		binary.LittleEndian.PutUint32(rec[0:4], uint32(i))
		if v == nil {
			binary.LittleEndian.PutUint32(rec[4:8], ^uint32(0))
		} else {
			binary.LittleEndian.PutUint32(rec[4:8], uint32(len(v)))
		}
		if _, e := w.Write(rec[:]); e != nil {
			return e
		}
		if v != nil {
			_, e = w.Write(v)
		}
		return e
	}
	if full {
		for i := range state {
			if e = emit(i); e != nil {
				break
			}
		}
	} else {
		for _, i := range dirty {
			if e = emit(i); e != nil {
				break
			}
		}
	}
	if gz != nil {
		ee := gz.Close()
		if e == nil {
			e = ee
		}
	}
	ee := b.Flush()
	if e == nil {
		e = ee
	}
	ee = f.Sync()
	if e == nil {
		e = ee
	}
	ee = f.Close()
	if e == nil {
		e = ee
	}
	if e != nil {
		return 0, 0, e
	}
	info, e := os.Stat(p)
	if e != nil {
		return 0, 0, e
	}
	return info.Size(), time.Since(st).Seconds(), nil
}
func apply(p string, state [][]byte, compress bool) (float64, error) {
	st := time.Now()
	f, e := os.Open(p)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	var r io.Reader = bufio.NewReaderSize(f, 1<<20)
	if compress {
		gz, err := gzip.NewReader(r)
		if err != nil {
			return 0, err
		}
		defer gz.Close()
		r = gz
	}
	var hdr [16]byte
	if _, e = io.ReadFull(r, hdr[:]); e != nil {
		return 0, e
	}
	if binary.LittleEndian.Uint64(hdr[0:8]) != magic {
		return 0, errors.New("bad magic")
	}
	count := binary.LittleEndian.Uint64(hdr[8:16])
	var rec [8]byte
	for i := uint64(0); i < count; i++ {
		if _, e = io.ReadFull(r, rec[:]); e != nil {
			return 0, e
		}
		k := binary.LittleEndian.Uint32(rec[0:4])
		sz := binary.LittleEndian.Uint32(rec[4:8])
		if int(k) >= len(state) {
			return 0, errors.New("bad key index")
		}
		if sz == ^uint32(0) {
			state[k] = nil
			continue
		}
		if sz > 8192 {
			return 0, errors.New("bad length")
		}
		v := make([]byte, sz)
		if _, e = io.ReadFull(r, v); e != nil {
			return 0, e
		}
		state[k] = v
	}
	return time.Since(st).Seconds(), nil
}
func digest(state [][]byte) string {
	h := sha256.New()
	var rec [8]byte
	for i, v := range state {
		binary.LittleEndian.PutUint32(rec[0:4], uint32(i))
		binary.LittleEndian.PutUint32(rec[4:8], uint32(len(v)))
		h.Write(rec[:])
		if v != nil {
			h.Write(v)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
func makeVal(i, step int, compress bool) []byte {
	v := make([]byte, keyLen)
	x := uint64(i+1)*0x9e3779b97f4a7c15 ^ uint64(step+13)*0xc2b2ae3d27d4eb4f
	for j := 0; j < len(v); j++ {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		if compress {
			v[j] = byte((uint64(i)*17 + uint64(step)*97 + uint64(j%8)) % 256)
		} else {
			v[j] = byte((x * 2685821657736338717) >> 56)
		}
	}
	return v
}
func scanAll(state [][]byte) float64 {
	st := time.Now()
	h := sha256.New()
	for _, v := range state {
		h.Write(v)
	}
	_ = h.Sum(nil)
	return time.Since(st).Seconds()
}
func execute(s Scenario) (Result, error) {
	res := Result{Scenario: s, Checkpoints: nSteps, RecoveryCorrect: true}
	n := (s.MiB << 20) / keyLen
	res.Keys = n
	state := make([][]byte, n)
	for i := range state {
		state[i] = makeVal(i, 0, s.Compress)
	}
	dir, e := os.MkdirTemp("", "cp-verify-")
	if e != nil {
		return res, e
	}
	defer os.RemoveAll(dir)
	base := filepath.Join(dir, "base")
	_, _, e = save(base, state, nil, true, s.Compress)
	if e != nil {
		return res, e
	}
	var fullPaths, deltaPaths, adaptPaths []string
	adaptBase := base
	deltaBase := base
	_, _ = deltaBase, adaptBase
	rng := rand.New(rand.NewSource(0x20261019))
	for step := 1; step <= nSteps; step++ {
		st := time.Now()
		nChanged := int(float64(n) * s.Churn)
		if s.Churn > 0 && nChanged == 0 {
			nChanged = 1
		}
		perm := rng.Perm(n)
		dirty := perm[:nChanged]
		sort.Ints(dirty)
		for _, i := range dirty { // small share of updates become tombstones, but keep most values for constant-size case
			if step%4 == 0 && (i%19 == 0) {
				state[i] = nil
			} else {
				state[i] = makeVal(i, step, s.Compress)
			}
		}
		res.MergeSec += time.Since(st).Seconds()
		p := filepath.Join(dir, fmt.Sprintf("full-%02d", step))
		dp := filepath.Join(dir, fmt.Sprintf("d-%02d", step))
		// Crossover ordering: half the checkpoints serialize incrementals first.
		isFull := (step%compactEvery == 0)
		var nb, db int64
		var tt, dt float64
		var err error
		if step%2 == 0 {
			db, dt, err = save(dp, state, dirty, isFull, s.Compress)
			if err != nil {
				return res, err
			}
			nb, tt, err = save(p, state, nil, true, s.Compress)
			if err != nil {
				return res, err
			}
		} else {
			nb, tt, err = save(p, state, nil, true, s.Compress)
			if err != nil {
				return res, err
			}
			db, dt, err = save(dp, state, dirty, isFull, s.Compress)
			if err != nil {
				return res, err
			}
		}
		res.BaselineSec += tt
		res.FullBytes += nb
		res.FullWrites++
		fullPaths = append(fullPaths, p)
		res.IncrementalSec += dt
		res.DeltaBytes += db
		deltaPaths = append(deltaPaths, dp)
		if isFull {
			deltaBase = dp
			deltaPaths = []string{dp}
		}
		// actual full scan is an optimistic lower bound for a real complete diff, because it does not compare two versions
		ss := scanAll(state)
		res.ScanSec += ss
		res.ScanDeltaSec += dt + ss
		adaptiveFull := isFull || s.Churn > 0.3
		ap := filepath.Join(dir, fmt.Sprintf("a-%02d", step))
		ab, at, err := save(ap, state, dirty, adaptiveFull, s.Compress)
		if err != nil {
			return res, err
		}
		res.AdaptiveSec += at
		res.AdaptiveBytes += ab
		adaptPaths = append(adaptPaths, ap)
		if adaptiveFull {
			res.AdaptiveFullWrites++
			adaptBase = ap
			adaptPaths = []string{ap}
		} else {
			res.DeltaWrites++
		}
		// Checkpoint-level correctness: compare restorable incremental state with final full state at steps 3 and 8.
		if step == 3 || step == nSteps {
			latest := make([][]byte, n)
			t, err := apply(p, latest, s.Compress)
			if err != nil {
				return res, err
			}
			res.BaselineRestoreSec += t
			ds := make([][]byte, n)
			t, err = apply(deltaBase, ds, s.Compress)
			if err != nil {
				return res, err
			}
			res.DeltaRestoreSec += t
			for _, file := range deltaPaths {
				if file == deltaBase {
					continue
				}
				t, err = apply(file, ds, s.Compress)
				if err != nil {
					return res, err
				}
				res.DeltaRestoreSec += t
			}
			as := make([][]byte, n)
			t, err = apply(adaptBase, as, s.Compress)
			if err != nil {
				return res, err
			}
			res.AdaptiveRestoreSec += t
			for _, file := range adaptPaths {
				if file == adaptBase {
					continue
				}
				t, err = apply(file, as, s.Compress)
				if err != nil {
					return res, err
				}
				res.AdaptiveRestoreSec += t
			}
			target := digest(state)
			if target != digest(latest) || target != digest(ds) || target != digest(as) {
				res.RecoveryCorrect = false
				return res, fmt.Errorf("mismatch step %d", step)
			}
		}
		// Clean redundant full objects from earlier steps, but leave recent objects needed for recovery
		if step > 3 {
			os.Remove(fullPaths[step-4])
		}
		if step == compactEvery { // base files are still needed only if restore has been checked already
			// Old delta files are removed only once a new full base is present (not production-grade GC)
			for z := 1; z < step; z++ {
				os.Remove(filepath.Join(dir, fmt.Sprintf("d-%02d", z)))
			}
		}
	}
	res.Notes = "local synced files; binary length-prefix; gzip-1 optional; incremental mutation list captured during common merge; no Substreams integration"
	return res, nil
}
func main() {
	out := flag.String("out", "results.json", "results")
	mib := flag.Int("mib", 48, "base state MiB")
	single := flag.Float64("churn", -1, "run one churn fraction, default sweep")
	coSingle := flag.Bool("compress", false, "compression for single case")
	flag.Parse()
	scenarios := []Scenario{}
	if *single >= 0 {
		scenarios = append(scenarios, Scenario{MiB: *mib, Churn: *single, Compress: *coSingle, Mode: "paired"})
	} else {
		for _, co := range []bool{false, true} {
			for _, c := range []float64{0.001, 0.01, 0.1, 0.5, 0.9} {
				scenarios = append(scenarios, Scenario{MiB: *mib, Churn: c, Compress: co, Mode: "paired"})
			}
		}
	}
	results := []Result{}
	for i, s := range scenarios {
		r, err := execute(s)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ERR", err)
			os.Exit(1)
		}
		results = append(results, r)
		fmt.Fprintf(os.Stderr, "[%d/%d] %.1f MiB churn=%.3f compressed=%v full=%.3fs delta=%.3fs scan=%.3fs correct=%v\n", i+1, len(scenarios), float64(s.MiB), s.Churn, s.Compress, r.BaselineSec, r.IncrementalSec, r.ScanSec, r.RecoveryCorrect)
		runtime.GC()
	}
	b, _ := json.MarshalIndent(results, "", "  ")
	if err := os.WriteFile(*out, b, 0644); err != nil {
		panic(err)
	}
	fmt.Println(*out)
}

var _ hash.Hash
