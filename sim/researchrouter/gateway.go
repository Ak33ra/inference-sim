// Package researchrouter contains deliberately scoped, cache-aware research
// routing policies. It is not the production llm-d router. PrebleE2 ports the
// block-prefix E2 placement cost only: it excludes Preble's prefill/decode
// balancing shortcut, prefix replication/load adjustment, and local scheduling.
package researchrouter

import (
	"container/list"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/cluster"
	"github.com/inference-sim/inference-sim/sim/internal/hash"
)

const (
	PolicyJSQ                = "jsq"
	PolicyLeastToken         = "least-token"
	PolicyRandom             = "random"
	PolicyAgentix            = "agentix"
	PolicyPrebleE2           = "preble-e2"
	PolicySessionReservation = "session-reservation"
)

// Config is the intentionally small, strict-JSON research-router contract.
// Times are microseconds except HistoryWindowS.
type Config struct {
	Policy               string
	LatencyUs            int64
	CacheEventDelayUs    int64
	HistoryWindowS       float64
	PrefillUsPerToken    float64
	DecodeUsPerToken     float64
	InitialOutputTokens  float64
	SessionClassManifest string               `json:",omitempty"`
	ReservedInstances    []cluster.InstanceID `json:",omitempty"`
}

// DefaultHistoryWindowS is Preble E2's published default three-minute window.
const DefaultHistoryWindowS = 180.0

// Validate makes policy configurations explicit and prevents a zero-value
// pricing model from silently looking like a Preble experiment.
func (c Config) Validate() error {
	switch c.Policy {
	case PolicyJSQ, PolicyLeastToken, PolicyRandom, PolicyAgentix, PolicyPrebleE2, PolicySessionReservation:
	default:
		return fmt.Errorf("research router policy must be one of jsq, least-token, random, agentix, preble-e2, session-reservation; got %q", c.Policy)
	}
	if c.LatencyUs < 0 || c.CacheEventDelayUs < 0 {
		return fmt.Errorf("research router latency values must be non-negative")
	}
	if c.HistoryWindowS <= 0 || math.IsNaN(c.HistoryWindowS) || math.IsInf(c.HistoryWindowS, 0) {
		return fmt.Errorf("research router history_window_s must be finite and > 0")
	}
	if c.PrefillUsPerToken < 0 || math.IsNaN(c.PrefillUsPerToken) || math.IsInf(c.PrefillUsPerToken, 0) ||
		c.DecodeUsPerToken < 0 || math.IsNaN(c.DecodeUsPerToken) || math.IsInf(c.DecodeUsPerToken, 0) ||
		c.InitialOutputTokens < 0 || math.IsNaN(c.InitialOutputTokens) || math.IsInf(c.InitialOutputTokens, 0) {
		return fmt.Errorf("research router pricing and initial_output_tokens must be finite and non-negative")
	}
	if c.Policy == PolicyPrebleE2 {
		if c.PrefillUsPerToken <= 0 || math.IsNaN(c.PrefillUsPerToken) || math.IsInf(c.PrefillUsPerToken, 0) {
			return fmt.Errorf("preble-e2 requires finite prefill_us_per_token > 0")
		}
		if c.DecodeUsPerToken <= 0 || math.IsNaN(c.DecodeUsPerToken) || math.IsInf(c.DecodeUsPerToken, 0) {
			return fmt.Errorf("preble-e2 requires finite decode_us_per_token > 0")
		}
	}
	return c.validateSessionReservation()
}

type cacheBlock struct {
	tokens int64
	lru    *list.Element
}

type heldRequest struct {
	req    *sim.Request
	cancel func()
}

type activeRequest struct {
	instance cluster.InstanceID
	tokens   int64
}

type workRecord struct {
	at           int64
	missedTokens int64
}

type completion struct {
	at     int64
	tokens int64
}

type hitReceipt struct {
	at   int64
	hash string
}

// Decision is a serializable decision explanation. The gateway keeps it in
// memory so callers can choose their own artifact without changing metrics.
type Decision struct {
	RequestID      string             `json:"request_id"`
	SessionID      string             `json:"session_id,omitempty"`
	SessionClass   string             `json:"session_class,omitempty"`
	At             int64              `json:"at_us"`
	Policy         string             `json:"policy"`
	Instance       cluster.InstanceID `json:"instance"`
	Mode           string             `json:"mode"`
	CachedTokens   int64              `json:"cached_tokens"`
	UncachedTokens int64              `json:"uncached_tokens"`
	Cost           float64            `json:"cost"`
}

// Stats makes policy behavior inspectable without mixing experimental counters
// into the simulator's stable production metrics schema.
type Stats struct {
	Decisions        int `json:"decisions"`
	KVStores         int `json:"kv_stores"`
	KVRemovals       int `json:"kv_removals"`
	ExploitDecisions int `json:"exploit_decisions"`
	ExploreDecisions int `json:"explore_decisions"`
}

// Gateway gives every research policy one arrival, cache-directory, and
// dispatch path. It only learns cache state through completed engine KV events.
type Gateway struct {
	cfg    Config
	host   cluster.GatewayHost
	rng    *rand.Rand
	tieRNG *rand.Rand

	pending  map[string]*heldRequest
	active   map[string]activeRequest
	terminal map[string]bool
	loads    map[cluster.InstanceID]int64
	tokens   map[cluster.InstanceID]int64

	// cache is an observed, delayed cache directory; it is never populated by a
	// direct engine cache read.
	cache        map[cluster.InstanceID]map[string]*cacheBlock
	lru          map[cluster.InstanceID]*list.List
	lastDelivery map[cluster.InstanceID]int64
	capacity     map[cluster.InstanceID]int64

	programs       map[sim.ProgramID]cluster.InstanceID
	history        map[cluster.InstanceID][]workRecord
	historyHead    map[cluster.InstanceID]int
	historyMissed  map[cluster.InstanceID]int64
	completions    map[cluster.InstanceID][]completion
	completionHead map[cluster.InstanceID]int
	completionSum  map[cluster.InstanceID]int64
	hitReceipts    map[cluster.InstanceID][]hitReceipt
	hitHead        map[cluster.InstanceID]int
	hitCounts      map[cluster.InstanceID]map[string]int

	reservation *sessionReservation

	decisions []Decision
	stats     Stats
}

// New creates a Gateway. Start supplies the host and subscribes it to engine
// events; no policy is allowed to query live engine cache state.
func New(cfg Config) (*Gateway, error) {
	if cfg.HistoryWindowS == 0 {
		cfg.HistoryWindowS = DefaultHistoryWindowS
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Gateway{
		cfg:            cfg,
		pending:        make(map[string]*heldRequest),
		active:         make(map[string]activeRequest),
		terminal:       make(map[string]bool),
		loads:          make(map[cluster.InstanceID]int64),
		tokens:         make(map[cluster.InstanceID]int64),
		cache:          make(map[cluster.InstanceID]map[string]*cacheBlock),
		lru:            make(map[cluster.InstanceID]*list.List),
		lastDelivery:   make(map[cluster.InstanceID]int64),
		capacity:       make(map[cluster.InstanceID]int64),
		programs:       make(map[sim.ProgramID]cluster.InstanceID),
		history:        make(map[cluster.InstanceID][]workRecord),
		historyHead:    make(map[cluster.InstanceID]int),
		historyMissed:  make(map[cluster.InstanceID]int64),
		completions:    make(map[cluster.InstanceID][]completion),
		completionHead: make(map[cluster.InstanceID]int),
		completionSum:  make(map[cluster.InstanceID]int64),
		hitReceipts:    make(map[cluster.InstanceID][]hitReceipt),
		hitHead:        make(map[cluster.InstanceID]int),
		hitCounts:      make(map[cluster.InstanceID]map[string]int),
	}, nil
}

// Start implements cluster.Gateway.
func (g *Gateway) Start(host cluster.GatewayHost) error {
	g.host = host
	g.rng = host.RNG().ForSubsystem("research-router/random")
	g.tieRNG = host.RNG().ForSubsystem("research-router/tie")
	for _, inst := range host.Instances() {
		role := host.PoolRole(inst.ID())
		if role != 0 && role != cluster.PoolRolePrefillDecode {
			return fmt.Errorf("research router supports only homogeneous shared replicas, found %s on %s", role, inst.ID())
		}
		g.subscribe(inst)
	}
	if g.cfg.Policy == PolicySessionReservation {
		return g.startSessionReservation()
	}
	return nil
}

func (g *Gateway) subscribe(inst *cluster.InstanceSimulator) {
	if _, ok := g.cache[inst.ID()]; ok {
		return
	}
	g.cache[inst.ID()] = make(map[string]*cacheBlock)
	g.lru[inst.ID()] = list.New()
	g.hitCounts[inst.ID()] = make(map[string]int)
	// Capacity is static deployment information read before routing; mutable
	// occupancy is represented solely by observed KV events.
	g.capacity[inst.ID()] = inst.TotalKVBlocks()
	inst.SetKVEventSink(func(at int64, events []sim.KVEvent) {
		copied := append([]sim.KVEvent(nil), events...)
		deliver := at + g.cfg.CacheEventDelayUs
		if deliver < g.lastDelivery[inst.ID()] {
			deliver = g.lastDelivery[inst.ID()]
		}
		g.lastDelivery[inst.ID()] = deliver
		g.host.Schedule(deliver, cluster.PriorityGatewayBackground, true, func() {
			g.applyKVEvents(inst.ID(), copied)
		})
	})
}

func (g *Gateway) applyKVEvents(id cluster.InstanceID, events []sim.KVEvent) {
	for _, e := range events {
		if e.Tier != "" { // E2 in this port makes GPU-local placement decisions.
			continue
		}
		if e.Removed {
			if old := g.cache[id][e.Hash]; old != nil {
				g.lru[id].Remove(old.lru)
				delete(g.cache[id], e.Hash)
			}
			g.stats.KVRemovals++
			continue
		}
		if old := g.cache[id][e.Hash]; old != nil {
			old.tokens = int64(len(e.Tokens))
			g.lru[id].MoveToBack(old.lru)
		} else {
			block := &cacheBlock{tokens: int64(len(e.Tokens))}
			block.lru = g.lru[id].PushBack(e.Hash)
			g.cache[id][e.Hash] = block
		}
		g.stats.KVStores++
	}
}

// OnArrival implements cluster.Gateway. Each policy sees the request at exactly
// the same scheduled decision time.
func (g *Gateway) OnArrival(req *sim.Request) {
	h := &heldRequest{req: req}
	g.pending[req.ID] = h
	g.host.Schedule(g.host.Now()+g.cfg.LatencyUs, cluster.PriorityGatewayRequest, false, func() {
		if g.pending[req.ID] != h {
			return
		}
		delete(g.pending, req.ID)
		if h.cancel != nil {
			h.cancel()
		}
		g.dispatch(req)
	})
	if req.Deadline > 0 {
		deadline := req.Deadline
		if deadline < g.host.Now() {
			deadline = g.host.Now()
		}
		h.cancel = g.host.Schedule(deadline, cluster.PriorityGatewayRequest, true, func() {
			if g.pending[req.ID] != h {
				return
			}
			delete(g.pending, req.ID)
			g.host.Reject(req, cluster.RejectedClientTimeout, "client deadline passed before research router decision")
		})
	}
}

func (g *Gateway) dispatch(req *sim.Request) {
	candidates := g.candidates()
	if len(candidates) == 0 && g.cfg.Policy != PolicySessionReservation {
		g.host.Reject(req, cluster.RejectedRouting, "research router found no routable endpoint")
		return
	}
	hashes := hash.ComputeBlockHashes(int(g.host.BlockSizeTokens()), req.InputTokens)
	id, mode, cached, cost, scores := g.choose(req, hashes, candidates)
	if id == "" { // choose has reported the terminal configuration error.
		return
	}
	missed := int64(len(req.InputTokens)) - cached
	if missed < 0 {
		missed = 0
	}
	decision := Decision{RequestID: req.ID, At: g.host.Now(), Policy: g.cfg.Policy, Instance: id, Mode: mode, CachedTokens: cached, UncachedTokens: missed, Cost: cost}
	if g.reservation != nil {
		decision.SessionID = string(req.SessionID)
		decision.SessionClass = g.reservation.sessions[decision.SessionID].Class
	}
	g.decisions = append(g.decisions, decision)
	g.stats.Decisions++
	if mode == "preble-exploit" {
		g.stats.ExploitDecisions++
	}
	if mode == "preble-explore" {
		g.stats.ExploreDecisions++
	}
	g.host.Dispatch(req, cluster.Placement{Decode: id, Scores: scores})
}

func (g *Gateway) candidates() []cluster.InstanceID {
	ids := make([]cluster.InstanceID, 0)
	for _, inst := range g.host.Instances() {
		if inst.IsRoutable() {
			ids = append(ids, inst.ID())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (g *Gateway) choose(req *sim.Request, hashes []string, ids []cluster.InstanceID) (cluster.InstanceID, string, int64, float64, map[string]float64) {
	switch g.cfg.Policy {
	case PolicySessionReservation:
		return g.sessionReservation(req, hashes, ids)
	case PolicyRandom:
		id := ids[g.rng.Intn(len(ids))]
		return id, "random", g.cachedTokens(id, hashes), 0, nil
	case PolicyJSQ:
		id := g.minLoad(ids, false)
		return id, "jsq", g.cachedTokens(id, hashes), float64(g.loads[id]), g.loadScores(ids, false)
	case PolicyLeastToken:
		id := g.minLoad(ids, true)
		return id, "least-token", g.cachedTokens(id, hashes), float64(g.tokens[id]), g.loadScores(ids, true)
	case PolicyAgentix:
		return g.agentix(req, hashes, ids)
	case PolicyPrebleE2:
		return g.preble(req, hashes, ids)
	default:
		panic("validated research router policy became invalid")
	}
}

func (g *Gateway) minLoad(ids []cluster.InstanceID, tokenLoad bool) cluster.InstanceID {
	best := ids[0]
	bestValue := g.loads[best]
	equal := 1
	if tokenLoad {
		bestValue = g.tokens[best]
	}
	for _, id := range ids[1:] {
		v := g.loads[id]
		if tokenLoad {
			v = g.tokens[id]
		}
		if v < bestValue {
			best, bestValue = id, v
			equal = 1
		} else if v == bestValue {
			equal++
			if g.tieRNG.Intn(equal) == 0 {
				best = id
			}
		}
	}
	return best
}

func (g *Gateway) loadScores(ids []cluster.InstanceID, tokenLoad bool) map[string]float64 {
	scores := make(map[string]float64, len(ids))
	for _, id := range ids {
		v := g.loads[id]
		if tokenLoad {
			v = g.tokens[id]
		}
		scores[string(id)] = -float64(v)
	}
	return scores
}

func (g *Gateway) agentix(req *sim.Request, hashes []string, ids []cluster.InstanceID) (cluster.InstanceID, string, int64, float64, map[string]float64) {
	if len(req.InputTokens) <= 2048 {
		id := g.minLoad(ids, false)
		return id, "agentix-short", g.cachedTokens(id, hashes), float64(g.loads[id]), g.loadScores(ids, false)
	}
	if req.ProgramID == "" {
		g.host.Abort(fmt.Errorf("agentix request %s has no ProgramID", req.ID))
		return "", "", 0, 0, nil
	}
	if id, ok := g.programs[req.ProgramID]; ok {
		for _, candidate := range ids {
			if candidate == id {
				return id, "agentix-affinity", g.cachedTokens(id, hashes), float64(g.loads[id]), g.loadScores(ids, false)
			}
		}
		g.host.Abort(fmt.Errorf("agentix program %q affinity target %q is no longer routable", req.ProgramID, id))
		return "", "", 0, 0, nil
	}
	id := g.minLoad(ids, false)
	g.programs[req.ProgramID] = id
	return id, "agentix-bind", g.cachedTokens(id, hashes), float64(g.loads[id]), g.loadScores(ids, false)
}

func (g *Gateway) preble(req *sim.Request, hashes []string, ids []cluster.InstanceID) (cluster.InstanceID, string, int64, float64, map[string]float64) {
	g.expire(ids)
	bestCache := int64(-1)
	var exploit []cluster.InstanceID
	for _, id := range ids {
		c := g.cachedTokens(id, hashes)
		if c > bestCache {
			bestCache, exploit = c, []cluster.InstanceID{id}
		} else if c == bestCache {
			exploit = append(exploit, id)
		}
	}
	if bestCache > int64(len(req.InputTokens))-bestCache {
		id, cost, scores := g.minimumE2Cost(exploit, hashes, int64(len(req.InputTokens)))
		return id, "preble-exploit", bestCache, cost, scores
	}
	best, bestCost, scores := g.minimumE2Cost(ids, hashes, int64(len(req.InputTokens)))
	return best, "preble-explore", g.cachedTokens(best, hashes), bestCost, scores
}

func (g *Gateway) minimumE2Cost(ids []cluster.InstanceID, hashes []string, promptTokens int64) (cluster.InstanceID, float64, map[string]float64) {
	best := ids[0]
	bestCost := math.Inf(1)
	equal := 0
	scores := make(map[string]float64, len(ids))
	for _, id := range ids {
		cached := g.cachedTokens(id, hashes)
		cost := g.recentCost(id) + g.evictionCost(id, cached, promptTokens) + g.cfg.PrefillUsPerToken*float64(promptTokens-cached)
		scores[string(id)] = -cost
		if cost < bestCost {
			best, bestCost = id, cost
			equal = 1
		} else if cost == bestCost {
			equal++
			if g.tieRNG.Intn(equal) == 0 {
				best = id
			}
		}
	}
	return best, bestCost, scores
}

func (g *Gateway) cachedTokens(id cluster.InstanceID, hashes []string) int64 {
	var blocks int64
	for _, h := range hashes {
		if _, ok := g.cache[id][h]; !ok {
			break
		}
		blocks++
	}
	return blocks * g.host.BlockSizeTokens()
}

func (g *Gateway) historyStart() int64 { return g.host.Now() - int64(g.cfg.HistoryWindowS*1_000_000) }

// expire removes a receipt once, at the next E2 decision after it falls out of
// the history window. Counts and sums make every subsequent cost lookup O(1).
func (g *Gateway) expire(ids []cluster.InstanceID) {
	start := g.historyStart()
	for _, id := range ids {
		for g.historyHead[id] < len(g.history[id]) && g.history[id][g.historyHead[id]].at < start {
			g.historyMissed[id] -= g.history[id][g.historyHead[id]].missedTokens
			g.historyHead[id]++
		}
		for g.completionHead[id] < len(g.completions[id]) && g.completions[id][g.completionHead[id]].at < start {
			g.completionSum[id] -= g.completions[id][g.completionHead[id]].tokens
			g.completionHead[id]++
		}
		for g.hitHead[id] < len(g.hitReceipts[id]) && g.hitReceipts[id][g.hitHead[id]].at < start {
			hash := g.hitReceipts[id][g.hitHead[id]].hash
			g.hitCounts[id][hash]--
			if g.hitCounts[id][hash] == 0 {
				delete(g.hitCounts[id], hash)
			}
			g.hitHead[id]++
		}
		g.history[id], g.historyHead[id] = compactReceipts(g.history[id], g.historyHead[id])
		g.completions[id], g.completionHead[id] = compactReceipts(g.completions[id], g.completionHead[id])
		g.hitReceipts[id], g.hitHead[id] = compactReceipts(g.hitReceipts[id], g.hitHead[id])
	}
}

// compactReceipts releases expired prefixes in occasional batches. Each item is
// copied at most once after its last use, keeping expiry amortized O(1) while
// bounding the mega-trace history to its active time window.
func compactReceipts[T any](items []T, head int) ([]T, int) {
	if head < 1024 || head*2 < len(items) {
		return items, head
	}
	if head == len(items) {
		return nil, 0
	}
	// Allocate the retained half so the old mega-trace backing array can be
	// collected instead of merely shortening a slice that still owns it.
	return append([]T(nil), items[head:]...), 0
}

func (g *Gateway) expectedOutput(id cluster.InstanceID) float64 {
	count := len(g.completions[id]) - g.completionHead[id]
	if count == 0 {
		return g.cfg.InitialOutputTokens
	}
	return float64(g.completionSum[id]) / float64(count)
}

func (g *Gateway) recentCost(id cluster.InstanceID) float64 {
	count := len(g.history[id]) - g.historyHead[id]
	return g.cfg.PrefillUsPerToken*float64(g.historyMissed[id]) + g.cfg.DecodeUsPerToken*g.expectedOutput(id)*float64(count)
}

// evictionCost plans against only the observed directory. Its router-side LRU
// approximation preserves observed store and routed-hit order; later real KV
// removals correct the directory. It never looks into a running engine step.
func (g *Gateway) evictionCost(id cluster.InstanceID, cached, promptTokens int64) float64 {
	blockSize := g.host.BlockSizeTokens()
	newBlocks := (promptTokens+blockSize-1)/blockSize - cached/blockSize
	need := int64(len(g.cache[id])) + newBlocks - g.capacity[id]
	if need <= 0 {
		return 0
	}
	denom := len(g.history[id]) - g.historyHead[id]
	if denom == 0 {
		return 0
	}
	var cost float64
	for e := g.lru[id].Front(); e != nil && need > 0; e = e.Next() {
		hash := e.Value.(string)
		block := g.cache[id][hash]
		hitRate := float64(g.hitCounts[id][hash]) / float64(denom)
		cost += g.cfg.PrefillUsPerToken * float64(block.tokens) * hitRate
		need--
	}
	return cost
}

// OnDispatched implements cluster.Gateway.
func (g *Gateway) OnDispatched(req *sim.Request, at int64) {
	if at != g.host.Now() {
		g.host.Abort(fmt.Errorf("research router: dispatch of %s reports future or stale time %d at clock %d", req.ID, at, g.host.Now()))
		return
	}
	id := cluster.InstanceID(req.AssignedInstance)
	hashes := hash.ComputeBlockHashes(int(g.host.BlockSizeTokens()), req.InputTokens)
	cached := g.cachedTokens(id, hashes)
	missed := int64(len(req.InputTokens)) - cached
	if missed < 0 {
		missed = 0
	}
	g.active[req.ID] = activeRequest{instance: id, tokens: int64(len(req.InputTokens))}
	g.loads[id]++
	g.tokens[id] += int64(len(req.InputTokens))
	if g.cfg.Policy == PolicyPrebleE2 {
		g.history[id] = append(g.history[id], workRecord{at: at, missedTokens: missed})
		g.historyMissed[id] += missed
		for _, h := range hashes {
			g.hitReceipts[id] = append(g.hitReceipts[id], hitReceipt{at: at, hash: h})
			g.hitCounts[id][h]++
		}
		for _, h := range hashes[:cached/g.host.BlockSizeTokens()] {
			g.lru[id].MoveToBack(g.cache[id][h].lru)
		}
	}
}

// OnFirstToken implements cluster.Gateway. The outstanding ledger deliberately
// stays until OnDone so every policy observes identical completed-work state.
func (g *Gateway) OnFirstToken(*sim.Request, int64) {}

// OnDone implements cluster.Gateway.
func (g *Gateway) OnDone(req *sim.Request, at int64, outcome cluster.DoneOutcome) {
	a, ok := g.active[req.ID]
	if !ok || g.terminal[req.ID] {
		return
	}
	// Engine steps may report their terminal callback before the DES clock reaches
	// the callback timestamp. Capture the already-emitted count, but do not let a
	// later arrival observe completion or output information early.
	g.terminal[req.ID] = true
	generated := int64(req.GeneratedTokens)
	publishAt := at
	if publishAt < g.host.Now() {
		publishAt = g.host.Now()
	}
	g.host.Schedule(publishAt, cluster.PriorityGatewayRequest, false, func() {
		g.publishDone(req.ID, a, publishAt, outcome, generated)
	})
}

func (g *Gateway) publishDone(id string, a activeRequest, at int64, outcome cluster.DoneOutcome, generated int64) {
	if !g.terminal[id] {
		return
	}
	delete(g.terminal, id)
	delete(g.active, id)
	g.loads[a.instance]--
	g.tokens[a.instance] -= a.tokens
	if g.cfg.Policy == PolicyPrebleE2 && outcome == cluster.DoneCompleted {
		g.completions[a.instance] = append(g.completions[a.instance], completion{at: at, tokens: generated})
		g.completionSum[a.instance] += generated
	}
}

// OnInstanceChange implements cluster.Gateway.
func (g *Gateway) OnInstanceChange(inst *cluster.InstanceSimulator) { g.subscribe(inst) }

// Pending implements cluster.Gateway.
func (g *Gateway) Pending() int { return len(g.pending) }

// Stop implements cluster.Gateway.
func (g *Gateway) Stop() {}

// Decisions returns a copy of the audit trail accumulated during this run.
func (g *Gateway) Decisions() []Decision { return append([]Decision(nil), g.decisions...) }

// Stats returns the audit counters for this run.
func (g *Gateway) Stats() Stats { return g.stats }

// WriteAudit writes the bounded study artifact only when the replay explicitly
// asks for it. The normal simulator metrics and llm-d path remain unchanged.
func (g *Gateway) WriteAudit(path string) error {
	data, err := json.MarshalIndent(struct {
		Config    Config     `json:"config"`
		Stats     Stats      `json:"stats"`
		Decisions []Decision `json:"decisions"`
	}{Config: g.cfg, Stats: g.Stats(), Decisions: g.Decisions()}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode research router audit: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write research router audit %q: %w", path, err)
	}
	return nil
}
