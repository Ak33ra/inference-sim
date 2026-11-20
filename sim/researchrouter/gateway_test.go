package researchrouter

import (
	"container/list"
	"math/rand"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/cluster"
	"github.com/inference-sim/inference-sim/sim/internal/hash"
)

type scheduled struct {
	at       int64
	priority int
	order    int
	cancel   bool
	fn       func()
}

// witnessHost can expose a model completion callback before its callback time,
// matching a cluster step that advances its engine past the DES clock.
type witnessHost struct {
	now       int64
	blockSize int64
	rng       *sim.PartitionedRNG
	events    []*scheduled
	aborted   error
}

func newWitnessHost() *witnessHost {
	return &witnessHost{blockSize: 4, rng: sim.NewPartitionedRNG(sim.NewSimulationKey(42))}
}

func (h *witnessHost) Now() int64 { return h.now }
func (h *witnessHost) Schedule(at int64, priority int, _ bool, fn func()) func() {
	e := &scheduled{at: at, priority: priority, order: len(h.events), fn: fn}
	h.events = append(h.events, e)
	return func() { e.cancel = true }
}
func (*witnessHost) Instances() []*cluster.InstanceSimulator                       { return nil }
func (*witnessHost) EndpointRanks(cluster.InstanceID) []*cluster.InstanceSimulator { return nil }
func (*witnessHost) Seed() int64                                                   { return 42 }
func (h *witnessHost) RNG() *sim.PartitionedRNG                                    { return h.rng }
func (h *witnessHost) BlockSizeTokens() int64                                      { return h.blockSize }
func (*witnessHost) PoolRole(cluster.InstanceID) cluster.PoolRole                  { return 0 }
func (*witnessHost) Dispatch(*sim.Request, cluster.Placement)                      {}
func (*witnessHost) Reject(*sim.Request, cluster.Rejection, string)                {}
func (*witnessHost) Evict(*sim.Request) bool                                       { return false }
func (h *witnessHost) Abort(err error)                                             { h.aborted = err }

func (h *witnessHost) runThrough(at int64) {
	for {
		var next *scheduled
		for _, e := range h.events {
			if e.cancel || e.at > at {
				continue
			}
			if next == nil || e.at < next.at || (e.at == next.at && (e.priority < next.priority || (e.priority == next.priority && e.order < next.order))) {
				next = e
			}
		}
		if next == nil {
			break
		}
		next.cancel = true
		h.now = next.at
		next.fn()
	}
	h.now = at
}

func testGateway(t *testing.T, cfg Config) (*Gateway, *witnessHost) {
	t.Helper()
	h := newWitnessHost()
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Start(h); err != nil {
		t.Fatal(err)
	}
	return g, h
}

func addReplica(g *Gateway, id cluster.InstanceID, capacity int64) {
	g.cache[id] = make(map[string]*cacheBlock)
	g.lru[id] = list.New()
	g.capacity[id] = capacity
	g.hitCounts[id] = make(map[string]int)
}

func TestMinLoadUsesReservoirSamplingForEqualCandidates(t *testing.T) {
	ids := []cluster.InstanceID{"a", "b", "c", "d"}
	wantRNG := rand.New(rand.NewSource(11)) //nolint:gosec // deterministic simulation fixture
	want := ids[0]
	for i := 1; i < len(ids); i++ {
		if wantRNG.Intn(i+1) == 0 {
			want = ids[i]
		}
	}
	g := &Gateway{tieRNG: rand.New(rand.NewSource(11)), loads: map[cluster.InstanceID]int64{}, tokens: map[cluster.InstanceID]int64{}} //nolint:gosec // deterministic simulation fixture
	if got := g.minLoad(ids, false); got != want {
		t.Fatalf("equal-load choice = %q, want reservoir result %q", got, want)
	}
}

func TestFutureCompletionReceiptStaysVisibleToCrossReplicaDecision(t *testing.T) {
	g, h := testGateway(t, Config{Policy: PolicyPrebleE2, HistoryWindowS: 180, PrefillUsPerToken: 1, DecodeUsPerToken: 1, InitialOutputTokens: 7})
	a, b := cluster.InstanceID("a"), cluster.InstanceID("b")
	g.active["done-later"] = activeRequest{instance: a, tokens: 16}
	g.loads[a], g.tokens[a] = 1, 16
	req := &sim.Request{ID: "done-later", GeneratedTokens: 99}

	g.OnDone(req, 200, cluster.DoneCompleted) // callback from a step starting at t=0
	if got := g.loads[a]; got != 1 {
		t.Fatalf("future completion changed a load at t=%d: got %d, want 1", h.now, got)
	}
	if got := g.expectedOutput(a); got != 7 {
		t.Fatalf("future completion leaked output at t=%d: got %.0f, want cold prior 7", h.now, got)
	}
	h.runThrough(150)
	if got := g.minLoad([]cluster.InstanceID{a, b}, false); got != b {
		t.Fatalf("arrival during a step chose %q, want still-busy peer %q", got, b)
	}
	h.runThrough(200)
	if got := g.loads[a]; got != 0 {
		t.Fatalf("completion receipt load = %d, want 0", got)
	}
	if got := g.expectedOutput(a); got != 99 {
		t.Fatalf("completion receipt output = %.0f, want 99", got)
	}
}

func TestAgentixUsesThresholdThenProgramAffinity(t *testing.T) {
	g, _ := testGateway(t, Config{Policy: PolicyAgentix, HistoryWindowS: 180})
	a, b := cluster.InstanceID("a"), cluster.InstanceID("b")
	ids := []cluster.InstanceID{a, b}
	g.loads[a], g.loads[b] = 4, 0
	short := &sim.Request{ID: "short", InputTokens: make([]sim.TokenID, 2048)}
	if id, mode, _, _, _ := g.agentix(short, nil, ids); id != b || mode != "agentix-short" {
		t.Fatalf("short call = (%q,%s), want least-used b", id, mode)
	}
	long := &sim.Request{ID: "long-1", ProgramID: "family", InputTokens: make([]sim.TokenID, 2049)}
	if id, mode, _, _, _ := g.agentix(long, nil, ids); id != b || mode != "agentix-bind" {
		t.Fatalf("first long call = (%q,%s), want bind b", id, mode)
	}
	g.loads[a], g.loads[b] = 0, 100
	long2 := &sim.Request{ID: "long-2", ProgramID: "family", InputTokens: make([]sim.TokenID, 4096)}
	if id, mode, _, _, _ := g.agentix(long2, nil, ids); id != b || mode != "agentix-affinity" {
		t.Fatalf("same-family long call = (%q,%s), want pinned b", id, mode)
	}
}

func TestObservedCacheDirectoryWaitsForDeliveryAndRemoval(t *testing.T) {
	g, h := testGateway(t, Config{Policy: PolicyJSQ, HistoryWindowS: 180})
	id := cluster.InstanceID("a")
	addReplica(g, id, 8)
	tokens := []sim.TokenID{1, 2, 3, 4}
	hash := hash.ComputeBlockHashes(4, tokens)[0]
	h.Schedule(10, cluster.PriorityGatewayBackground, false, func() { g.applyKVEvents(id, []sim.KVEvent{{Hash: hash, Tokens: tokens}}) })
	if got := g.cachedTokens(id, []string{hash}); got != 0 {
		t.Fatalf("cache indexed before observed event: %d tokens", got)
	}
	h.runThrough(9)
	if got := g.cachedTokens(id, []string{hash}); got != 0 {
		t.Fatalf("cache indexed before delivery time: %d tokens", got)
	}
	h.runThrough(10)
	if got := g.cachedTokens(id, []string{hash}); got != 4 {
		t.Fatalf("cache after store = %d tokens, want 4", got)
	}
	h.Schedule(20, cluster.PriorityGatewayBackground, false, func() { g.applyKVEvents(id, []sim.KVEvent{{Hash: hash, Removed: true}}) })
	h.runThrough(20)
	if got := g.cachedTokens(id, []string{hash}); got != 0 {
		t.Fatalf("cache after observed removal = %d tokens, want 0", got)
	}
}

func TestPrebleExploitExploreAndFullPrefixPopularity(t *testing.T) {
	g, h := testGateway(t, Config{Policy: PolicyPrebleE2, HistoryWindowS: 180, PrefillUsPerToken: 1, DecodeUsPerToken: 10, InitialOutputTokens: 7})
	a, b := cluster.InstanceID("a"), cluster.InstanceID("b")
	addReplica(g, a, 8)
	addReplica(g, b, 8)
	prompt := []sim.TokenID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	hashes := hash.ComputeBlockHashes(4, prompt)
	g.applyKVEvents(a, []sim.KVEvent{{Hash: hashes[0], Tokens: prompt[:4]}, {Hash: hashes[1], Parent: hashes[0], Tokens: prompt[4:8]}})
	req := &sim.Request{ID: "exploit", InputTokens: prompt}
	if id, mode, cached, _, _ := g.preble(req, hashes, []cluster.InstanceID{a, b}); id != a || mode != "preble-exploit" || cached != 8 {
		t.Fatalf("exploit = (%q,%s,%d), want (a,preble-exploit,8)", id, mode, cached)
	}
	g.applyKVEvents(a, []sim.KVEvent{{Hash: hashes[1], Removed: true}})
	g.history[a] = append(g.history[a], workRecord{at: h.now, missedTokens: 100})
	g.historyMissed[a] = 100
	if id, mode, _, _, _ := g.preble(req, hashes, []cluster.InstanceID{a, b}); id != b || mode != "preble-explore" {
		t.Fatalf("explore = (%q,%s), want (b,preble-explore)", id, mode)
	}
	dispatched := &sim.Request{ID: "popularity", AssignedInstance: string(a), InputTokens: prompt}
	g.OnDispatched(dispatched, h.now)
	if got := len(g.hitReceipts[a]); got != len(hashes) {
		t.Fatalf("prefix popularity receipts = %d, want all %d full prompt blocks", got, len(hashes))
	}
	if h.aborted != nil {
		t.Fatal(h.aborted)
	}
}

func TestPrebleExploitBreaksEqualPrefixOnFullE2Cost(t *testing.T) {
	g, h := testGateway(t, Config{Policy: PolicyPrebleE2, HistoryWindowS: 180, PrefillUsPerToken: 1, DecodeUsPerToken: 1, InitialOutputTokens: 1})
	a, b := cluster.InstanceID("a"), cluster.InstanceID("b")
	addReplica(g, a, 8)
	addReplica(g, b, 8)
	prompt := []sim.TokenID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	hashes := hash.ComputeBlockHashes(4, prompt)
	events := []sim.KVEvent{{Hash: hashes[0], Tokens: prompt[:4]}, {Hash: hashes[1], Parent: hashes[0], Tokens: prompt[4:8]}}
	g.applyKVEvents(a, events)
	g.applyKVEvents(b, events)
	g.history[a] = append(g.history[a], workRecord{at: h.now, missedTokens: 100})
	g.historyMissed[a] = 100
	if id, mode, _, _, _ := g.preble(&sim.Request{ID: "tie", InputTokens: prompt}, hashes, []cluster.InstanceID{a, b}); id != b || mode != "preble-exploit" {
		t.Fatalf("equal-prefix exploit = (%q,%s), want lower-E2-cost b", id, mode)
	}
}

func TestExpireUsesQueuesAndPreservesCounts(t *testing.T) {
	g, h := testGateway(t, Config{Policy: PolicyPrebleE2, HistoryWindowS: 1, PrefillUsPerToken: 1, DecodeUsPerToken: 1, InitialOutputTokens: 1})
	id := cluster.InstanceID("a")
	addReplica(g, id, 1)
	g.history[id] = []workRecord{{at: 0, missedTokens: 4}, {at: 2_000_000, missedTokens: 8}}
	g.historyMissed[id] = 12
	g.hitReceipts[id] = []hitReceipt{{at: 0, hash: "old"}, {at: 2_000_000, hash: "new"}}
	g.hitCounts[id]["old"], g.hitCounts[id]["new"] = 1, 1
	h.now = 2_000_000
	g.expire([]cluster.InstanceID{id})
	if got := g.historyMissed[id]; got != 8 || g.hitCounts[id]["old"] != 0 || g.hitCounts[id]["new"] != 1 {
		t.Fatalf("expiry state = missed %d, hits %#v", got, g.hitCounts[id])
	}
}
