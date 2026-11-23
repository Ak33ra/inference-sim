package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
)

func splitTestGateway(reqs []*sim.Request, remaining []int64) *SessionSplitGateway {
	q := newSessionSplitGateway(splitManifest{Requests: map[string]splitReference{}}, 0)
	for i, r := range reqs {
		q.manifest.Requests[r.ID] = splitReference{RuntimeSessionID: r.SessionID, Step: r.RoundIndex, RemainingUs: remaining[i], ReferenceRun: "isolated"}
	}
	return q
}
func splitTestRequests(n int) []*sim.Request {
	var out []*sim.Request
	for i := 0; i < n; i++ {
		out = append(out, &sim.Request{ID: fmt.Sprintf("r%d", i), SessionID: fmt.Sprintf("f/s%d", i), InputTokens: make([]sim.TokenID, 4), OutputTokens: make([]sim.TokenID, 4), MaxOutputLen: 4, State: sim.StateQueued})
	}
	return out
}
func splitTestConfig(n int) DeploymentConfig {
	c := newTestDeploymentConfig(n)
	c.PrefixCachingDisabled = true
	c.MaxNumSeqs = 2
	c.MaxNumBatchedTokens = 8
	c.LatencyModel = testFakeZeroQueueing()
	return c
}
func verifySplitChoices(t *testing.T, q *SessionSplitGateway) {
	t.Helper()
	ready := map[string]SplitEvent{}
	claimed := map[string]bool{}
	for _, e := range q.events {
		switch e.Kind {
		case "ready":
			ready[e.RequestID] = e
		case "claim":
			if claimed[e.RequestID] {
				t.Fatalf("duplicate claim %s", e.RequestID)
			}
			if len(ready) != e.ReadyCount {
				t.Fatalf("ready count %d != %d", len(ready), e.ReadyCount)
			}
			for _, r := range ready {
				before := r.RemainingUs < e.RemainingUs
				if e.LargestFirst {
					before = r.RemainingUs > e.RemainingUs
				}
				if r.RemainingUs == e.RemainingUs {
					before = r.ArrivalUs < e.ArrivalUs || r.ArrivalUs == e.ArrivalUs && r.RequestID < e.RequestID
				}
				if before {
					t.Fatalf("claimed %s before ready %s", e.RequestID, r.RequestID)
				}
			}
			if _, ok := ready[e.RequestID]; !ok {
				t.Fatalf("unready claim %s", e.RequestID)
			}
			if e.AtUs < e.PreparedUs || e.PreparedUs < e.ArrivalUs {
				t.Fatal("negative wait")
			}
			if e.Evidence.UsedKVBlocks > e.Evidence.PromisedKVBlocks || e.Evidence.PromisedKVBlocks > e.Evidence.TotalKVBlocks {
				t.Fatal("invalid KV promise")
			}
			delete(ready, e.RequestID)
			claimed[e.RequestID] = true
		}
	}
	if len(ready) > 0 || len(claimed) != len(q.manifest.Requests) {
		t.Fatalf("unfinished ready/claim coverage %d/%d", len(ready), len(claimed))
	}
}
func TestSessionSplit_NativeBatchingAndCentralDelay(t *testing.T) {
	reqs := splitTestRequests(6)
	q := splitTestGateway(reqs, []int64{100, 90, 80, 30, 20, 10})
	q.latencyUs = 7
	c := splitTestConfig(2)
	c.LatencyModel = testFakeLatency()
	cs := NewClusterSimulator(c, NewSliceRequestSource(reqs), nil)
	cs.SetGateway(q)
	mustRun(t, cs)
	verifySplitChoices(t, q)
	claims := []SplitEvent{}
	for _, e := range q.events {
		if e.Kind == "claim" {
			claims = append(claims, e)
		}
	}
	if claims[0].RequestID != "r0" || claims[1].RequestID != "r1" || claims[2].RequestID != "r5" || claims[3].RequestID != "r4" {
		t.Fatalf("wrong initial extremes: %+v", claims[:4])
	}
	if claims[1].Evidence.BatchSize != 2 || claims[3].Evidence.BatchSize != 2 {
		t.Fatal("native batching was serialized")
	}
	prepared := int64(7) + c.LatencyModel.QueueingTime(reqs[0])
	if claims[0].AtUs != prepared {
		t.Fatalf("preparation charged at %d, want %d", claims[0].AtUs, prepared)
	}
	m := cs.AggregatedMetrics()
	if m.TotalInputTokens != 24 || m.PreemptionCount != 0 || m.KVAllocationFailures != 0 {
		t.Fatalf("incorrect accounting/preemption: %d/%d/%d", m.TotalInputTokens, m.PreemptionCount, m.KVAllocationFailures)
	}
	waited := false
	for _, e := range claims {
		inst := cs.instanceByID(e.Instance)
		rm := inst.Metrics().Requests[e.RequestID]
		if rm.HandledBy != string(e.Instance) || rm.GatewayQueueDelay != float64(e.AtUs-e.ArrivalUs)/1000 {
			t.Fatal("dispatch attribution missing from registered metrics")
		}
		if inst.Metrics().RequestSchedulingDelays[e.RequestID] != e.AtUs-e.ArrivalUs {
			t.Fatal("central wait missing from scheduling delay")
		}
		if e.AtUs > e.PreparedUs {
			waited = true
		}
	}
	if !waited {
		t.Fatal("fixture did not exercise central waiting")
	}
}
func TestSessionSplit_ZeroPreparationSequentialSuccessor(t *testing.T) {
	r := splitTestRequests(2)
	r[1].SessionID = r[0].SessionID
	r[1].RoundIndex = 1
	q := splitTestGateway(r, []int64{100, 50})
	ref := q.manifest.Requests[r[1].ID]
	ref.PreviousRequestID = r[0].ID
	q.manifest.Requests[r[1].ID] = ref
	c := splitTestConfig(2)
	var cs *ClusterSimulator
	cs = NewClusterSimulator(c, NewSliceRequestSource(r[:1]), func(req *sim.Request, at int64) []*sim.Request {
		for _, inst := range cs.Instances() {
			if done, ok := inst.Metrics().RequestCompletionTimes[req.ID]; ok {
				at = max(at, int64(done))
			}
		}
		if req == r[0] {
			r[1].ArrivalTime = at
			return r[1:]
		}
		return nil
	})
	cs.SetGateway(q)
	mustRun(t, cs)
	verifySplitChoices(t, q)
	var done, arrival, claim int64
	for _, e := range q.events {
		if e.RequestID == r[0].ID && e.Kind == "complete" {
			done = e.AtUs
		}
		if e.RequestID == r[1].ID && e.Kind == "ready" {
			arrival = e.AtUs
		}
		if e.RequestID == r[1].ID && e.Kind == "claim" {
			claim = e.AtUs
		}
	}
	if done != arrival || arrival != claim {
		t.Fatalf("same-time successor stalled: done=%d ready=%d claim=%d", done, arrival, claim)
	}
}
func TestSessionSplit_FailedAdmissionRetainsCentralOwnership(t *testing.T) {
	r := splitTestRequests(2)
	for _, req := range r {
		req.OutputTokens = make([]sim.TokenID, 8)
		req.MaxOutputLen = 8
	}
	q := splitTestGateway(r, []int64{100, 50})
	c := splitTestConfig(1)
	c.TotalKVBlocks = 3
	c.BlockSizeTokens = 4
	cs := NewClusterSimulator(c, NewSliceRequestSource(r), nil)
	cs.SetGateway(q)
	mustRun(t, cs)
	verifySplitChoices(t, q)
	var secondClaim int64
	for _, e := range q.events {
		if e.Kind == "claim" && e.RequestID == r[0].ID {
			if e.Evidence.UsedKVBlocks != 1 || e.Evidence.PromisedKVBlocks != 3 {
				t.Fatalf("promise allocated physical KV: %+v", e.Evidence)
			}
		}

		if e.Kind == "claim" && e.RequestID == r[1].ID {
			secondClaim = e.AtUs
		}
	}
	if secondClaim < r[0].DepartureTime {
		t.Fatal("failed admission acquired ownership")
	}
	if cs.AggregatedMetrics().PreemptionCount != 0 {
		t.Fatal("request was preempted")
	}
	if cs.instances[0].sim.KVCache.(sim.LifecycleKVStore).PromisedBlocks() != 0 {
		t.Fatal("reservation leak")
	}
}
func TestSessionSplit_RejectsDeadline(t *testing.T) {
	r := splitTestRequests(1)
	r[0].Deadline = 1
	q := splitTestGateway(r, []int64{100})
	cs := NewClusterSimulator(splitTestConfig(1), NewSliceRequestSource(r), nil)
	cs.SetGateway(q)
	if err := cs.Run(); err == nil {
		t.Fatal("deadline accepted")
	}
}

func TestSessionSplit_PreservesChunkingWithCentralWaiters(t *testing.T) {
	r := splitTestRequests(2)
	for _, req := range r {
		req.InputTokens = make([]sim.TokenID, 12)
	}
	q := splitTestGateway(r, []int64{100, 50})
	c := splitTestConfig(1)
	c.MaxNumBatchedTokens = 16
	c.LongPrefillTokenThreshold = 4
	cs := NewClusterSimulator(c, NewSliceRequestSource(r), nil)
	cs.SetGateway(q)
	mustRun(t, cs)
	for _, e := range q.events {
		if e.Kind == "claim" {
			if e.Evidence.ScheduledTokens != 4 {
				t.Fatalf("central waiters did not retain prefill cap: %+v", e)
			}
			break
		}
	}
}

func TestSessionSplit_RejectsExistingEngineWork(t *testing.T) {
	r := splitTestRequests(1)
	q := splitTestGateway(r, []int64{100})
	cs := NewClusterSimulator(splitTestConfig(1), NewSliceRequestSource(nil), nil)
	cs.instances[0].sim.InjectArrival(r[0])
	cs.SetGateway(q)
	if err := cs.Run(); err == nil {
		t.Fatal("accepted an engine with previously injected work")
	}
}

type countedSplitPreparation struct {
	sim.LatencyModel
	calls int
}

func (m *countedSplitPreparation) QueueingTime(req *sim.Request) int64 {
	m.calls++
	return m.LatencyModel.QueueingTime(req)
}

func TestSessionSplit_NativeRendererContentionBeforeGPUClaim(t *testing.T) {
	r := splitTestRequests(4)
	q := splitTestGateway(r, []int64{10, 40, 20, 30})
	q.latencyUs = 7
	c := splitTestConfig(2)
	c.Frontend = sim.FrontendConfig{RendererNumWorkers: 1}
	m := &countedSplitPreparation{LatencyModel: queueingFake(100)}
	c.LatencyModel = m
	cs := NewClusterSimulator(c, NewSliceRequestSource(r), nil)
	cs.SetGateway(q)
	mustRun(t, cs)
	verifySplitChoices(t, q)
	if m.calls != len(r) {
		t.Fatalf("preparation charged %d times for %d requests", m.calls, len(r))
	}
	prepared := 0
	for _, e := range q.events {
		if e.Kind != "prepare" {
			continue
		}
		wait := int64(prepared / 2 * 100)
		if e.PreparationInstance != InstanceID(fmt.Sprintf("instance_%d", prepared%2)) || e.PreparationServiceUs != 100 || e.PreparationWaitUs != wait || e.PreparedUs != 7+100+wait {
			t.Fatalf("wrong native renderer schedule: %+v", e)
		}
		prepared++
	}
	if prepared != 4 || r[1].AssignedInstance != "instance_0" {
		t.Fatal("fixture did not separate preparation endpoint from largest-first GPU")
	}
	for _, inst := range cs.instances {
		renderer := inst.sim.Renderer()
		if renderer.Rendered() != 2 || renderer.WaitTicks() != 100 {
			t.Fatalf("native renderer accounting differs: %d/%d", renderer.Rendered(), renderer.WaitTicks())
		}
	}
}

func TestSessionSplit_RejectsUsedRenderer(t *testing.T) {
	c := splitTestConfig(1)
	c.Frontend = sim.FrontendConfig{RendererNumWorkers: 1}
	cs := NewClusterSimulator(c, NewSliceRequestSource(nil), nil)
	cs.instances[0].sim.Renderer().Admit(0, 1)
	cs.SetGateway(splitTestGateway(nil, nil))
	if err := cs.Run(); err == nil {
		t.Fatal("accepted a previously used renderer")
	}
}

func TestSessionSplit_RejectsMixedReferenceTimelines(t *testing.T) {
	m := splitManifest{FormatVersion: 1, TraceSHA256: fmt.Sprintf("%064x", 1), ReferenceLabel: "isolated", Requests: map[string]splitReference{
		"first": {TraceID: "f", SessionID: "s", RuntimeSessionID: "f/s", RemainingUs: 100, ReferenceRun: "ref", ReferenceElapsedUs: 100, ReferenceSessionCompletionUs: 100},
		"next":  {TraceID: "f", SessionID: "s", RuntimeSessionID: "f/s", Step: 1, RemainingUs: 50, ReferenceRun: "ref", ReferenceElapsedUs: 100, ReferenceArrivalUs: 50, ReferenceSessionCompletionUs: 100, PreviousRequestID: "first"},
	}}
	path := filepath.Join(t.TempDir(), "remaining.json")
	write := func() {
		t.Helper()
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write()
	if _, err := loadSplitManifest(path); err != nil {
		t.Fatal(err)
	}
	r := m.Requests["next"]
	r.ReferenceArrivalUs += 100
	r.ReferenceSessionCompletionUs += 100
	m.Requests["next"] = r
	write()
	if _, err := loadSplitManifest(path); err == nil {
		t.Fatal("accepted inconsistent reference session completion times")
	}
}
