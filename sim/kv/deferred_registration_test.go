package kv

import (
	"reflect"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
)

func TestDeferredKVRegistrationFailureAndCancellation(t *testing.T) {
	store := NewKVCacheState(1, 2)
	owner := remoteKVReq("owner", 1, 2)
	if !store.AllocateKVBlocksDeferred(owner, 0, 2, nil) {
		t.Fatal("initial allocation failed")
	}
	blocked := remoteKVReq("blocked", 3, 4)
	if store.AllocateKVBlocksDeferred(blocked, 0, 2, nil) || store.remoteKV[blocked.ID] || len(store.deferredFrom) != 1 {
		t.Fatal("failed admission retained registration state")
	}
	store.ReleaseKVBlocks(owner)
	if store.remoteKV[owner.ID] || len(store.deferredFrom) != 0 || len(store.GetCachedBlocks(owner.InputTokens)) != 0 {
		t.Fatal("canceled allocation published cache entries")
	}
	if !store.AllocateKVBlocksDeferred(blocked, 0, 2, nil) {
		t.Fatal("retry failed")
	}
	store.CommitKVBlocks(blocked, 2)
	if store.remoteKV[blocked.ID] || len(store.deferredFrom) != 0 || len(store.GetCachedBlocks(blocked.InputTokens)) != 1 {
		t.Fatal("committed allocation not published")
	}
	store.ReleaseKVBlocks(blocked)
	if err := store.verifyBlockConservation(); err != nil {
		t.Fatal(err)
	}
}

func TestDeferredKVPublicationMatchesRemoteFullPublication(t *testing.T) {
	local, remote := NewKVCacheState(32, 2), NewKVCacheState(32, 2)
	local.EnableKVEvents()
	remote.EnableKVEvents()
	req := remoteKVReq("first", 1, 2, 3, 4, 5, 6, 7, 8)
	req.OutputTokens = toks(20, 26)
	check := func() {
		t.Helper()
		if !reflect.DeepEqual(local.HashToBlock, remote.HashToBlock) || !reflect.DeepEqual(local.holders, remote.holders) {
			t.Fatal("suffix publication changed hash ownership")
		}
		if !reflect.DeepEqual(local.TakeKVEvents(), remote.TakeKVEvents()) {
			t.Fatal("suffix publication changed cache events or hash parents")
		}
		if len(local.deferredFrom) != 0 || len(local.remoteKV) != 0 {
			t.Fatal("commit retained deferred state")
		}
	}
	start := int64(0)
	for _, end := range []int64{3, 5, 8, 9, 10, 11, 12, 13, 14} {
		req.ProgressIndex = start
		if !local.AllocateKVBlocksDeferred(req, start, end, nil) || !remote.AllocateRemoteKV(req, start, end, nil) {
			t.Fatal("allocation failed")
		}
		local.CommitKVBlocks(req, end)
		remote.CacheRemoteKV(req.ID)
		check()
		start = end
	}
	local.ReleaseKVBlocks(req)
	remote.ReleaseKVBlocks(req)
	followup := remoteKVReq("followup", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	for _, store := range []*KVCacheState{local, remote} {
		cached := store.GetCachedBlocks(followup.InputTokens)
		if len(cached) != 4 {
			t.Fatalf("cached prefix has %d blocks, want 4", len(cached))
		}
		if store == local {
			if !store.AllocateKVBlocksDeferred(followup, 8, 10, cached) {
				t.Fatal("cached admission failed")
			}
			store.CommitKVBlocks(followup, 10)
		} else {
			if !store.AllocateRemoteKV(followup, 8, 10, cached) {
				t.Fatal("remote cached admission failed")
			}
			store.CacheRemoteKV(followup.ID)
		}
	}
	check()
	for _, store := range []*KVCacheState{local, remote} {
		store.ReleaseKVBlocks(followup)
		if err := store.verifyBlockConservation(); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkDeferredKVDecodePublication(b *testing.B) {
	for _, full := range []bool{false, true} {
		name := "suffix"
		if full {
			name = "full-prefix"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store := NewKVCacheState(2200, 16)
				req := &sim.Request{ID: "decode", InputTokens: toks(0, 32768), OutputTokens: toks(32768, 33280)}
				if !store.AllocateKVBlocks(req, 0, 32768, nil) {
					b.Fatal("prefill allocation failed")
				}
				for end := int64(32769); end <= 33280; end++ {
					req.ProgressIndex = end - 1
					if full {
						if !store.AllocateRemoteKV(req, end-1, end, nil) {
							b.Fatal("decode allocation failed")
						}
						store.CacheRemoteKV(req.ID)
					} else {
						if !store.AllocateKVBlocksDeferred(req, end-1, end, nil) {
							b.Fatal("decode allocation failed")
						}
						store.CommitKVBlocks(req, end)
					}
				}
			}
		})
	}
}

func TestDeferredKVRegistrationPublishesHybridTailsAfterExecution(t *testing.T) {
	store := newPrefixStore(v41NoReplay)
	req := &sim.Request{ID: "first", InputTokens: toks(0, 40)}
	store.ReconcilePrefixHit(req, nil)
	if !store.AllocateKVBlocksDeferred(req, 0, 40, nil) {
		t.Fatal("allocation failed")
	}
	if len(store.tails) != 0 || len(store.GetCachedBlocks(req.InputTokens)) != 0 {
		t.Fatal("unexecuted hashes or window tails were published")
	}
	store.CommitKVBlocks(req, 40)
	if got := admit(t, store, "followup", cat(req.InputTokens, toks(100, 120))); got != 32 {
		t.Fatalf("executed hybrid tail not reusable: got %d tokens", got)
	}
}
