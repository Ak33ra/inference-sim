package kernelmodel

import (
	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/kv"
	"testing"
)

func TestLifecyclePromiseCoversGPTOSSChunks(t *testing.T) {
	verifyLifecycle(t, open(t, "gpt-oss-120b-h200-fp4-vllm-tp1.yaml"))
}
func TestLifecyclePromiseCoversBoundedStateChunks(t *testing.T) {
	r := repos()
	r.Scenarios = "../../testdata/scenarios"
	m, err := Open(dspark, r)
	if err != nil {
		t.Fatal(err)
	}
	verifyLifecycle(t, m)
}
func verifyLifecycle(t *testing.T, m *Model) {
	t.Helper()
	b, err := m.KVBudget()
	if err != nil {
		t.Fatal(err)
	}
	state := m.sequenceState(b)
	if state.Hold != nil && state.LifecyclePeak == nil {
		t.Fatal("kernel omitted bounded-state peak")
	}
	for _, n := range []int64{17, 1025, 16384, 32769, 65536, 131072} {
		if state.Hold == nil {
			continue
		}
		peak := state.LifecyclePeak(n)
		for computed := int64(0); computed < n; computed += 16 {
			for _, chunk := range []int64{1, 16, 1024, 16384} {
				scheduled := min(chunk, n-computed)
				if held := state.Hold(computed, scheduled); held > peak {
					t.Fatalf("tokens=%d computed=%d scheduled=%d held=%d peak=%d", n, computed, scheduled, held, peak)
				}
			}
		}
	}
	k := kv.NewKVCacheState(200000, 16)
	k.SequenceState = state
	if err := k.EnableLifecycleReservations(); err != nil {
		t.Fatal(err)
	}
	r := &sim.Request{ID: "lifecycle", InputTokens: make([]sim.TokenID, 32769), OutputTokens: make([]sim.TokenID, 33), MaxOutputLen: 33}
	for r.ProgressIndex < r.InputLen() {
		end := min(r.ProgressIndex+1024, r.InputLen())
		if !k.AllocateKVBlocks(r, r.ProgressIndex, end, nil) {
			t.Fatal("prefill refused")
		}
		r.ProgressIndex = end
	}
	for r.ProgressIndex < r.InputLen()+33 {
		if !k.AllocateKVBlocks(r, r.ProgressIndex, r.ProgressIndex+1, nil) {
			t.Fatal("decode refused")
		}
		r.ProgressIndex++
	}
	k.ReleaseKVBlocks(r)
	if k.PromisedBlocks() != 0 || k.UsedBlocks() != 0 {
		t.Fatal("lifecycle leaked blocks")
	}
}
