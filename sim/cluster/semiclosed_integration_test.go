package cluster

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/inference-sim/inference-sim/sim"
	"github.com/inference-sim/inference-sim/sim/internal/testutil"
	"github.com/inference-sim/inference-sim/sim/internal/testutil/fakelatency"
	"github.com/inference-sim/inference-sim/sim/workload"
	"github.com/inference-sim/inference-sim/sim/workload/semiclosed"
)

func semiClosedFixture(t *testing.T, name string) *semiclosed.Replay {
	t.Helper()
	dir := "../workload/semiclosed/testdata/" + name
	if name == "source-parity" {
		dir = os.Getenv("BLIS_SEMICLOSED_SOURCE_FIXTURE")
	}
	c, err := semiclosed.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	x, err := semiclosed.NewReplay(c, semiclosed.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func runSemiClosedCluster(t *testing.T, cfg DeploymentConfig, name string, gate ArrivalGate) (*ClusterSimulator, semiclosed.Report, error) {
	t.Helper()
	x := semiClosedFixture(t, name)
	cfg.Model = "m"
	if name == "source-parity" {
		cfg.Model = "glm-5.2"
	}
	var cs *ClusterSimulator
	cs = NewClusterSimulator(cfg, NewSliceRequestSource(x.Start()), func(req *sim.Request, now int64) []*sim.Request {
		if req.State == sim.StateCompleted {
			for _, inst := range cs.Instances() {
				if at, ok := inst.Metrics().RequestCompletionTimes[req.ID]; ok {
					now = max(now, int64(at))
					break
				}
			}
		}
		return x.OnComplete(req, now)
	})
	cs.SetArrivalGate(func(req *sim.Request, now int64) (bool, []*sim.Request) {
		send, more := x.Gate(req, now)
		if send && gate != nil {
			keep, extra := gate(req, now)
			return keep, append(more, extra...)
		}
		return send, more
	})
	cs.SetUnservedHook(x.OnUnserved)
	mustRun(t, cs)
	err := x.Finish(cs.Clock(), cs.AggregatedMetrics())
	return cs, x.Summary(), err
}

func TestSemiClosedCausalDrainThroughCluster(t *testing.T) {
	for _, pd := range []bool{false, true} {
		t.Run(fmt.Sprintf("pd=%t", pd), func(t *testing.T) {
			cfg := newTestDeploymentConfig(2)
			fixture, count, zeros := "causal", 11, 1
			if pd {
				cfg = newTestDisaggDeploymentConfig(2, 1, 1)
				fixture, count, zeros = "parity", 6, 0
			}
			cs, report, err := runSemiClosedCluster(t, cfg, fixture, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !report.Complete || !report.AccountingMatch || report.States[semiclosed.Completed] != count || report.Selected.ZeroOutputRequests != zeros {
				t.Fatalf("failed drain: %+v", report)
			}
			assertClusterINV1Conservation(t, cs, count, cs.RejectedRequests(), "semi-closed")
			byID := make(map[string]semiclosed.RequestReport)
			for _, r := range report.Requests {
				byID[r.RequestID] = r
			}
			for _, r := range report.Requests {
				if r.ArrivalUs == nil || r.CompletionUs == nil {
					t.Fatalf("missing request lifetime: %+v", r)
				}
				if len(r.Prerequisites) > 0 {
					var anchor int64
					for _, p := range r.Prerequisites {
						prior := byID[p.RequestID]
						at := *prior.CompletionUs
						if p.On == "send" {
							at = *prior.ArrivalUs
						}
						anchor = max(anchor, at)
					}
					if *r.ArrivalUs != anchor+r.DelayUs {
						t.Errorf("%s arrival %d != dependencies %d + delay %d", r.RequestID, *r.ArrivalUs, anchor, r.DelayUs)
					}
				}
				metrics := cs.AggregatedMetrics()
				if r.EngineE2EUs == nil || r.EngineCompletionUs == nil || *r.EngineE2EUs != metrics.RequestE2Es[r.RequestID] || *r.EngineCompletionUs != metrics.RequestCompletionTimes[r.RequestID] {
					t.Fatalf("%s missing actual engine metrics", r.RequestID)
				}
				if !pd && (metrics.RequestE2Es[r.RequestID] != float64(*r.E2EUs) || metrics.RequestCompletionTimes[r.RequestID] != float64(*r.CompletionUs)) {
					t.Errorf("%s report disagrees with actual engine latency", r.RequestID)
				}
			}
			if pd {
				for _, parent := range cs.ParentRequests() {
					if *byID[parent.ID].CompletionUs != parent.CompletionTime {
						t.Errorf("%s did not use copied parent completion", parent.ID)
					}
					if parent.ID == "fixture/P/P/0" {
						t.Logf("P/D first root lifecycle completion=%d, engine metric completion=%.0f", parent.CompletionTime, *byID[parent.ID].EngineCompletionUs)
					}
				}
			}
			if !pd && *byID["fixture/B/B/0"].ArrivalUs != 2000 {
				t.Fatal("staggered root arrival changed")
			}
			if !pd && *report.Families[0].CompletionUs != *byID["fixture/A/c4/0"].CompletionUs {
				t.Fatal("background child failed to extend family lifetime")
			}
		})
	}
}

func TestSemiClosedRefusalThroughCluster(t *testing.T) {
	for _, pd := range []bool{false, true} {
		t.Run(fmt.Sprintf("pd=%t", pd), func(t *testing.T) {
			cfg := newTestDeploymentConfig(1)
			fixture, blocked, completed := "causal", 8, 2
			if pd {
				cfg = newTestDisaggDeploymentConfig(2, 1, 1)
				fixture, blocked, completed = "parity", 5, 0
			}
			cfg.MaxModelLen = 100
			_, report, err := runSemiClosedCluster(t, cfg, fixture, nil)
			if err == nil {
				t.Fatal("rejected source work passed accounting")
			}
			if report.States[semiclosed.Failed] != 1 || report.States[semiclosed.Blocked] != blocked || report.States[semiclosed.Completed] != completed || report.States[semiclosed.Censored] != 0 {
				t.Fatalf("refusal lifecycle: %v", report.States)
			}
		})
	}
}

func TestSemiClosedFixedCohortAgentXMetrics(t *testing.T) {
	for _, concurrency := range []int{1, 2} {
		fixture := "parity"
		if concurrency == 2 {
			fixture = "parity-two"
		}
		t.Run(fmt.Sprintf("families=%d", concurrency), func(t *testing.T) { compareSemiClosedAgentXCohort(t, concurrency, fixture) })
	}
}

func TestSemiClosedSourceFamilyAgentXMetrics(t *testing.T) {
	if os.Getenv("BLIS_SEMICLOSED_SOURCE_FIXTURE") == "" {
		t.Skip("set BLIS_SEMICLOSED_SOURCE_FIXTURE to the documented source-family conversion")
	}
	compareSemiClosedAgentXCohort(t, 1, "source-parity")
}

func TestSemiClosedSameTimeBackgroundAgentXMetrics(t *testing.T) {
	compareSemiClosedAgentXCohort(t, 1, "parity-background")
}

func compareSemiClosedAgentXCohort(t *testing.T, concurrency int, fixture string) {
	type wire struct {
		input, output []sim.TokenID
		arrival       int64
	}
	wires := make(map[string]wire)
	cfg := newTestDeploymentConfig(2)
	cfg.Model = "m"
	if fixture == "source-parity" {
		cfg.Model = "glm-5.2"
	}
	coeffs := testutil.DefaultFakeLatency()
	coeffs.OutputTokenProcessingTicks, coeffs.PostDecodeOverheadTicks = 0, 0
	cfg.LatencyModel = fakelatency.WithCoeffs(coeffs)
	semi, report, err := runSemiClosedCluster(t, cfg, fixture, func(req *sim.Request, now int64) (bool, []*sim.Request) {
		key := req.ID
		wires[key] = wire{append([]sim.TokenID(nil), req.InputTokens...), append([]sim.TokenID(nil), req.OutputTokens...), now}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	agentFixture := fixture
	if fixture == "parity-two" {
		agentFixture = "parity"
	}
	corpusPath := "../workload/semiclosed/testdata/" + agentFixture + "/corpus.jsonl"
	if fixture == "source-parity" {
		corpusPath = filepath.Join(os.Getenv("BLIS_SEMICLOSED_SOURCE_FIXTURE"), "corpus.jsonl")
	}
	data, err := os.ReadFile(corpusPath)
	if err != nil {
		t.Fatal(err)
	}
	corpus, err := workload.ReadAgentXCorpus(strings.NewReader(string(data)), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	x, err := workload.NewAgentXReplay(corpus, workload.AgentXReplayConfig{Concurrency: concurrency, Seed: 42, DurationUs: 3_600_000_000, GraceUs: math.MaxInt64, MinRatio: 0, MaxRatio: 0})
	if err != nil {
		t.Fatal(err)
	}
	var agx *ClusterSimulator
	completed := 0
	agx = NewClusterSimulator(cfg, NewSliceRequestSource(x.Start()), func(req *sim.Request, now int64) []*sim.Request {
		more := x.OnComplete(req, now)
		if req.Agentic != nil && req.Agentic.Tree < concurrency && req.State == sim.StateCompleted {
			completed++
			if completed == len(wires) {
				agx.SetHorizon(now)
			}
		}
		return more
	})
	ids := make(map[string]string)
	agx.SetArrivalGate(func(req *sim.Request, now int64) (bool, []*sim.Request) {
		send, more := x.Gate(req, now)
		if !send {
			return false, more
		}
		if req.Agentic.Tree >= concurrency {
			return false, nil
		}
		if req.Phase == sim.PhaseWarmup {
			t.Fatal("fixed cohort sent primer warmup")
		}
		family := req.Agentic.TraceID
		session := family[strings.LastIndex(family, "/")+1:]
		if req.Agentic.Kind != "root" {
			session = strings.TrimPrefix(req.Agentic.Stream, family+"::sa:")
		}
		if req.Agentic.Tree == 1 {
			family = "fixture/Q"
			session = strings.ReplaceAll(strings.ReplaceAll(session, "P", "Q"), "pc", "qc")
		}
		key := fmt.Sprintf("%s/%s/%d", family, session, req.Agentic.Turn)
		w, ok := wires[key]
		if !ok {
			t.Fatalf("unexpected AgentX request %s (%+v)", key, req.Agentic)
		}
		if now != w.arrival {
			t.Errorf("%s arrival %d != semi-closed %d", key, now, w.arrival)
		}
		req.InputTokens = append([]sim.TokenID(nil), w.input...)
		req.OutputTokens = append([]sim.TokenID(nil), w.output...)
		req.MaxOutputLen = len(w.output)
		req.Model = cfg.Model
		ids[key] = req.ID
		return true, more
	})
	agx.SetUnservedHook(x.OnUnserved)
	mustRun(t, agx)
	if completed != len(wires) || len(ids) != len(wires) {
		t.Fatalf("fixed cohort dropped or recycled work: completions=%d selected=%d", completed, len(wires))
	}
	left, right := semi.AggregatedMetrics(), agx.AggregatedMetrics()
	for key, id := range ids {
		original := key
		if left.RequestTTFTs[original] != right.RequestTTFTs[id] || left.RequestE2Es[original] != right.RequestE2Es[id] || left.RequestCompletionTimes[original] != right.RequestCompletionTimes[id] {
			t.Errorf("%s latency changed: TTFT %v/%v E2E %v/%v completion %v/%v", key, left.RequestTTFTs[original], right.RequestTTFTs[id], left.RequestE2Es[original], right.RequestE2Es[id], left.RequestCompletionTimes[original], right.RequestCompletionTimes[id])
		}
	}
	engineCounters := func(m *sim.Metrics) []any {
		return []any{m.CompletedRequests, m.TotalInputTokens, m.TotalOutputTokens, m.SimEndedTime, m.KVBlocksUsed, m.PeakKVBlocksUsed, m.PreemptionCount, m.KVAllocationFailures, m.PrefixCacheQueryTokens, m.PrefixCacheHitTokens, m.TTFTSum, m.ITLSum, m.DroppedUnservable, m.LengthCappedRequests, m.StillQueued, m.StillRunning}
	}
	if !reflect.DeepEqual(engineCounters(left), engineCounters(right)) {
		t.Errorf("engine counters differ\nsemi %v\nAgentX %v", engineCounters(left), engineCounters(right))
	}
	if !reflect.DeepEqual(left.BuildOutput("cluster"), right.BuildOutput("cluster")) {
		t.Error("aggregate engine output differs")
	}
	if left.SimEndedTime != report.EndUs || left.SimEndedTime != right.SimEndedTime {
		t.Fatal("measurement windows differ")
	}
	assertClusterINV1Conservation(t, agx, len(wires), agx.RejectedRequests(), "AgentX fixed cohort")
}
