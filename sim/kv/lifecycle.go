package kv

import (
	"fmt"
	"github.com/inference-sim/inference-sim/sim"
)

// EnableLifecycleReservations keeps admitted requests resident through completion.
func (k *KVCacheState) EnableLifecycleReservations() error {
	if k.SequenceState.Hold != nil && k.SequenceState.LifecyclePeak == nil {
		return fmt.Errorf("bounded KV state has no declared lifecycle peak")
	}
	if k.UsedBlocks() != 0 {
		return fmt.Errorf("lifecycle reservations must be enabled before allocation")
	}
	if k.lifecycle == nil {
		k.lifecycle = make(map[string]int64)
	}
	return nil
}

// LifecycleBlocks bounds full-attention and bounded-state occupancy through the client's
// output budget. The kernel's Hold(0,n) holds every page any chunk within n tokens can use.
func (k *KVCacheState) LifecycleBlocks(req *sim.Request) int64 {
	n := req.InputLen() + int64(req.MaxOutputLen)
	blocks := (n + k.BlockSizeTokens - 1) / k.BlockSizeTokens
	var state int64
	if k.SequenceState.LifecyclePeak != nil {
		state = k.SequenceState.LifecyclePeak(n)
	}
	if k.SequenceState.Gate != nil {
		state = max(state, k.SequenceState.Gate(n))
	}
	return blocks + state
}
func (k *KVCacheState) PromisedBlocks() int64 { return k.lifecycleTotal }

func (k *KVCacheState) allocateLifecycle(req *sim.Request, start, end int64, cached []int64) bool {
	promise, active := k.lifecycle[req.ID]
	if !active {
		promise = k.LifecycleBlocks(req)
		if promise+k.lifecycleTotal > k.TotalBlocks {
			k.lifecycleRefusals++
			return false
		}
	}
	if !k.allocate(req, start, start, end, cached) {
		panic(fmt.Sprintf("request %s physical KV allocation failed within lifecycle promise %d", req.ID, promise))
	}
	if !active {
		k.lifecycle[req.ID] = promise
		k.lifecycleTotal += promise
		k.lifecyclePeak = max(k.lifecyclePeak, k.lifecycleTotal)
	}
	held := int64(len(k.RequestMap[req.ID]) + len(k.StateBlocks[req.ID]) + len(k.SpareBlocks[req.ID]))
	if held > promise {
		panic(fmt.Sprintf("request %s holds %d KV blocks beyond lifecycle promise %d", req.ID, held, promise))
	}
	return true
}

func (k *KVCacheState) LifecycleStats() sim.LifecycleReservationStats {
	return sim.LifecycleReservationStats{FinalPromisedBlocks: k.lifecycleTotal, TotalKVBlocks: k.TotalBlocks, AdmissionRefusals: k.lifecycleRefusals, PeakPromisedBlocks: k.lifecyclePeak}
}
