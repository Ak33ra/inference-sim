package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
)

func requestSplitFixture(n int) ([]*sim.Request, requestServiceManifest) {
	reqs := splitTestRequests(n)
	hash := strings.Repeat("a", 64)
	m := requestServiceManifest{FormatVersion: 1, TraceSHA256: hash, BinarySHA256: hash, EngineSHA256: hash,
		ReferenceLabel: "isolated client service", SourceHashes: map[string]string{"oracle/workload.json": hash},
		Requests: map[string]requestServiceReference{}, Shapes: map[string]requestServiceShape{}}
	for i, req := range reqs {
		req.SessionID, req.RoundIndex = "f/s", i
		req.InputTokens = make([]sim.TokenID, 4+i)
		key := fmt.Sprintf("%d/4", 4+i)
		m.Shapes[key] = requestServiceShape{InputTokens: int64(4 + i), OutputTokens: 4, ServiceUs: int64(100 - i), OracleRequestID: fmt.Sprintf("oracle/%d", i)}
		m.Requests[req.ID] = requestServiceReference{TraceID: "f", SessionID: "s", RuntimeSessionID: "f/s", Step: i, Shape: key}
	}
	return reqs, m
}

func TestRequestSplit_ExactExtremesAndRequestOwnership(t *testing.T) {
	reqs, m := requestSplitFixture(18)
	q := newRequestSplitGateway(m, 0)
	c := splitTestConfig(8)
	c.MaxNumBatchedTokens = 64
	cs := NewClusterSimulator(c, NewSliceRequestSource(reqs), nil)
	cs.SetGateway(q)
	mustRun(t, cs)
	ready := map[string]SplitEvent{}
	claims := []SplitEvent{}
	for _, e := range q.events {
		switch e.Kind {
		case "ready":
			ready[e.RequestID] = e
		case "claim":
			if len(ready) != e.ReadyCount {
				t.Fatal("ready count differs")
			}
			for _, other := range ready {
				ahead := other.ServiceUs < e.ServiceUs
				if e.LargestFirst {
					ahead = other.ServiceUs > e.ServiceUs
				}
				if other.ServiceUs == e.ServiceUs {
					ahead = other.ArrivalUs < e.ArrivalUs || other.ArrivalUs == e.ArrivalUs && other.RequestID < e.RequestID
				}
				if ahead {
					t.Fatalf("claimed %s before %s", e.RequestID, other.RequestID)
				}
			}
			delete(ready, e.RequestID)
			claims = append(claims, e)
		}
	}
	if len(ready) != 0 || len(claims) != len(reqs) {
		t.Fatal("request accounting differs")
	}
	if claims[0].RequestID != "r0" || claims[1].RequestID != "r1" || claims[2].RequestID != "r17" || claims[3].RequestID != "r16" {
		t.Fatalf("wrong initial extrema: %+v", claims[:4])
	}
	if claims[1].Evidence.BatchSize != 2 || claims[1].AtUs != 0 {
		t.Fatal("requests sharing a session were serialized")
	}
	if cs.AggregatedMetrics().PreemptionCount != 0 {
		t.Fatal("request preempted")
	}
	for _, inst := range cs.instances {
		if inst.sim.KVCache.UsedBlocks() != 0 || inst.sim.KVCache.(sim.LifecycleKVStore).PromisedBlocks() != 0 {
			t.Fatal("KV promise leaked")
		}
	}
	path := filepath.Join(t.TempDir(), "audit.json")
	if err := q.WriteAudit(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"remaining_us"`) || strings.Contains(string(raw), `"reference_run"`) || !strings.Contains(string(raw), `"service_us"`) {
		t.Fatal("request audit contains session priority metadata")
	}
}

func TestRequestSplit_RejectsUnsupportedReplicaCount(t *testing.T) {
	reqs, m := requestSplitFixture(1)
	for _, n := range []int{1, 7, 9} {
		cs := NewClusterSimulator(splitTestConfig(n), NewSliceRequestSource(reqs), nil)
		cs.SetGateway(newRequestSplitGateway(m, 0))
		if err := cs.Run(); err == nil || !strings.Contains(err.Error(), "eight") {
			t.Fatalf("accepted %d replicas: %v", n, err)
		}
	}
}

func TestRequestSplit_RejectsRuntimeShapeBudgetOrIdentityMismatch(t *testing.T) {
	changes := map[string]func(*sim.Request){
		"input":   func(r *sim.Request) { r.InputTokens = append(r.InputTokens, 0) },
		"output":  func(r *sim.Request) { r.OutputTokens = append(r.OutputTokens, 0) },
		"budget":  func(r *sim.Request) { r.MaxOutputLen++ },
		"session": func(r *sim.Request) { r.SessionID = "other" },
		"step":    func(r *sim.Request) { r.RoundIndex++ },
		"unknown": func(r *sim.Request) { r.ID = "unknown" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			reqs, m := requestSplitFixture(1)
			change(reqs[0])
			cs := NewClusterSimulator(splitTestConfig(8), NewSliceRequestSource(reqs), nil)
			cs.SetGateway(newRequestSplitGateway(m, 0))
			if err := cs.Run(); err == nil {
				t.Fatal("accepted mismatched oracle request")
			}
		})
	}
}

func TestRequestSplit_StrictServiceManifest(t *testing.T) {
	_, m := requestSplitFixture(2)
	path := filepath.Join(t.TempDir(), "service.json")
	write := func(m requestServiceManifest) {
		t.Helper()
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(m)
	if _, err := NewRequestSplitGateway(path, 0); err != nil {
		t.Fatal(err)
	}
	shape := m.Shapes["4/4"]
	shape.ServiceUs = 0
	m.Shapes["4/4"] = shape
	write(m)
	if _, err := NewRequestSplitGateway(path, 0); err == nil {
		t.Fatal("accepted nonpositive service")
	}
	_, m = requestSplitFixture(2)
	ref := m.Requests["r1"]
	ref.Step = 0
	m.Requests["r1"] = ref
	write(m)
	if _, err := NewRequestSplitGateway(path, 0); err == nil {
		t.Fatal("accepted duplicate identity")
	}
}
