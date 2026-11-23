package kv

import (
	"github.com/inference-sim/inference-sim/sim"
	"testing"
)

func TestLifecycleReservations_RollbackAndBoundedState(t *testing.T) {
	k := NewKVCacheState(5, 4)
	k.SequenceState = sim.KVSequenceState{Hold: func(c, f int64) int64 { return 2 }, Gate: func(n int64) int64 { return 2 }, LifecyclePeak: func(n int64) int64 { return 2 }}
	if err := k.EnableLifecycleReservations(); err != nil {
		t.Fatal(err)
	}
	a := &sim.Request{ID: "a", InputTokens: make([]sim.TokenID, 4), OutputTokens: make([]sim.TokenID, 8), MaxOutputLen: 8}
	b := &sim.Request{ID: "b", InputTokens: make([]sim.TokenID, 4), MaxOutputLen: 4}
	if !k.AllocateKVBlocks(a, 0, 4, nil) {
		t.Fatal("first request refused")
	}
	if k.PromisedBlocks() != 5 || k.UsedBlocks() != 3 {
		t.Fatal("logical promise changed physical allocation")
	}
	if k.AllocateKVBlocks(b, 0, 4, nil) {
		t.Fatal("overcommitted lifecycle")
	}
	if k.PromisedBlocks() != 5 || len(k.RequestMap[b.ID]) != 0 {
		t.Fatal("failed admission leaked ownership")
	}
	a.ProgressIndex = 4
	for end := int64(5); end <= 12; end++ {
		if !k.AllocateKVBlocks(a, end-1, end, nil) {
			t.Fatalf("admitted request cannot finish at %d", end)
		}
		a.ProgressIndex = end
	}
	k.ReleaseKVBlocks(a)
	k.ReleaseKVBlocks(a)
	if k.PromisedBlocks() != 0 || k.UsedBlocks() != 0 {
		t.Fatal("release leaked or double-counted promise")
	}
	if !k.AllocateKVBlocks(b, 0, 4, nil) {
		t.Fatal("released capacity unavailable")
	}
}
func TestLifecycleReservations_RequiresDeclaredStateEnvelope(t *testing.T) {
	k := NewKVCacheState(10, 4)
	k.SequenceState.Hold = func(c, f int64) int64 { return 1 }
	if k.EnableLifecycleReservations() == nil {
		t.Fatal("accepted arbitrary Hold without a lifecycle bound")
	}
}

func TestLifecycleReservations_PhysicalFailureIsAnInvariantError(t *testing.T) {
	k := NewKVCacheState(4, 4)
	if err := k.EnableLifecycleReservations(); err != nil {
		t.Fatal(err)
	}
	r := &sim.Request{ID: "broken-store", InputTokens: make([]sim.TokenID, 4), MaxOutputLen: 4}
	k.FreeBlockCnt = 0
	defer func() {
		if recover() == nil {
			t.Fatal("physical allocation failure became a retryable capacity refusal")
		}
	}()
	k.AllocateKVBlocks(r, 0, 4, nil)
}
