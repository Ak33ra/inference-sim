package sim

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/inference-sim/inference-sim/sim/internal/testutil"
)

func feedbackRuntime(t *testing.T, mode string) *ResearchSchedulerRuntime {
	t.Helper()
	r, err := NewResearchSchedulerRuntime(mode, ResearchSchedulerConfig{[]float64{2, 4, 8}, 100})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func feedbackSim(t *testing.T, r *ResearchSchedulerRuntime, seqs, budget int64) *Simulator {
	t.Helper()
	cfg := msConfig(math.MaxInt64)
	cfg.BatchConfig = NewBatchConfig(seqs, budget, 16, WithPrefixCachingDisabled(true))
	cfg.ResearchScheduler = r
	cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
	return mustNewSimulator(t, cfg)
}
func feedbackRequest(id string, program ProgramID, input, output int, at int64) *Request {
	return &Request{ID: id, ProgramID: program, InputTokens: msMakeTokens(input), OutputTokens: msMakeTokens(output), ArrivalTime: at, State: StateQueued}
}
func TestResearchMLFQDisplacesAndPreservesWork(t *testing.T) {
	for _, input := range []int{1, 128} {
		t.Run(fmt.Sprint(input), func(t *testing.T) {
			run := func(runtime *ResearchSchedulerRuntime) *Simulator {
				s := feedbackSim(t, runtime, 1, 16)
				s.InjectArrival(feedbackRequest("long", "p", input, 20, 0))
				s.InjectArrival(feedbackRequest("short", "q", 1, 1, 2500))
				s.Run()
				return s
			}
			base := run(nil)
			runtime := feedbackRuntime(t, "mlfq")
			s := run(runtime)
			report := runtime.Snapshot()
			long := report.Requests["long"]
			if long.Displacements == 0 || long.Resumes == 0 || long.RecomputedTokens == 0 || long.Demotions == 0 {
				t.Fatalf("policy was not exercised: %+v", long)
			}
			if s.Metrics.RequestCompletionTimes["short"] >= base.Metrics.RequestCompletionTimes["short"] {
				t.Fatal("short call did not benefit from displacement")
			}
			if s.Metrics.CompletedRequests != 2 || s.Metrics.TotalInputTokens != input+1 || s.Metrics.TotalOutputTokens != 21 {
				t.Fatalf("changed work: %+v", s.Metrics)
			}
			for _, id := range []string{"long", "short"} {
				if !report.Requests[id].Completed {
					t.Fatal("incomplete", id)
				}
			}
			if input == 1 && s.Metrics.RequestTTFTs["long"] != base.Metrics.RequestTTFTs["long"] {
				t.Fatal("preemption replaced TTFT")
			}
			if s.KVCache.UsedBlocks() != 0 {
				t.Fatal("KV blocks leaked")
			}
		})
	}
}

func TestResearchSimultaneousBurstCompletesEachRequestOnce(t *testing.T) {
	for _, mode := range []string{"mlfq", "atlas"} {
		t.Run(mode, func(t *testing.T) {
			runtime := feedbackRuntime(t, mode)
			s := feedbackSim(t, runtime, 8, 64)
			for i := range 5 {
				s.InjectArrival(feedbackRequest(fmt.Sprint(i), "p", 16, 10, 0))
			}
			s.Run()
			if s.Metrics.CompletedRequests != 5 || s.Metrics.TotalInputTokens != 80 || s.Metrics.TotalOutputTokens != 50 {
				t.Fatalf("burst changed work: completed=%d input=%d output=%d", s.Metrics.CompletedRequests, s.Metrics.TotalInputTokens, s.Metrics.TotalOutputTokens)
			}
			for _, stats := range runtime.Snapshot().Requests {
				if !stats.Completed {
					t.Fatal("burst left an unfinished call")
				}
			}
		})
	}
}
func TestResearchATLASNoFutureHistoryAcrossEngines(t *testing.T) {
	runtime := feedbackRuntime(t, "atlas")
	a := feedbackSim(t, runtime, 1, 16)
	b := feedbackSim(t, runtime, 1, 16)
	first := feedbackRequest("first", "family", 1, 1, 0)
	a.EnqueueRequest(first)
	a.Step(0)
	if first.State != StateCompleted {
		t.Fatal("fixture must compute completion at future time1000")
	}
	second := feedbackRequest("second", "family", 1, 1, 500)
	(&ArrivalEvent{time: 500, Request: second}).Execute(b)
	if got := runtime.Report.Requests["second"].InheritedServiceUs; got != 0 {
		t.Fatalf("arrival inherited future service: %d", got)
	}
	third := feedbackRequest("third", "family", 1, 1, 1000)
	(&ArrivalEvent{time: 1000, Request: third}).Execute(b)
	if got := runtime.Report.Requests["third"].InheritedServiceUs; got != 1000 {
		t.Fatalf("completion did not become visible: %d", got)
	}
}
func TestResearchATLASParallelMaxAndFrozenInheritance(t *testing.T) {
	runtime := feedbackRuntime(t, "atlas")
	a, b := newResearchScheduler(runtime), newResearchScheduler(runtime)
	first := feedbackRequest("a", "family", 1, 1, 0)
	second := feedbackRequest("b", "family", 1, 1, 0)
	a.admit(first, 0)
	b.admit(second, 0)
	a.executed([]*Request{first}, 1000, 1000, nil)
	a.completed(first, 1000)
	b.executed([]*Request{second}, 3000, 3000, nil)
	b.completed(second, 3000)
	next := feedbackRequest("c", "family", 1, 1, 2000)
	a.admit(next, 2000)
	if a.calls[next.ID].stats.InheritedServiceUs != 1000 {
		t.Fatal("wrong arrival snapshot")
	}
	runtime.flush(3000)
	if runtime.Report.Programs["family"].ServiceUs != 3000 {
		t.Fatal("parallel work summed")
	}
	a.executed([]*Request{next}, 4000, 500, nil)
	a.completed(next, 4000)
	runtime.flush(4000)
	if runtime.Report.Programs["family"].ServiceUs != 3000 {
		t.Fatal("arrival inheritance was refreshed")
	}
}
func TestResearchPromotionPreservesLifetimeService(t *testing.T) {
	runtime := feedbackRuntime(t, "atlas")
	runtime.Report.Config.StarvationRatio = 1
	runtime.Report.Programs["p"] = ResearchProgramStats{ServiceUs: 6000}
	s := newResearchScheduler(runtime)
	r := feedbackRequest("r", "p", 1, 1, 0)
	s.admit(r, 0)
	s.calls[r.ID].stats.ModelServiceUs = 1234
	s.rank([]*Request{r}, 7000)
	c := s.calls[r.ID]
	if c.level != 0 || c.stats.Promotions != 1 || c.stats.QueueWaitUs != 7000 || c.stats.ModelServiceUs != 1234 || c.model != 0 || c.wait != 0 {
		t.Fatalf("incorrect promotion: %+v stats %+v", c, c.stats)
	}
}
func TestResearchMLFQIgnoresProgramIdentity(t *testing.T) {
	run := func(same bool) map[string]*ResearchCallStats {
		r := feedbackRuntime(t, "mlfq")
		s := feedbackSim(t, r, 1, 16)
		p := ProgramID("other")
		if same {
			p = "p"
		}
		s.InjectArrival(feedbackRequest("a", "p", 1, 12, 0))
		s.InjectArrival(feedbackRequest("b", p, 1, 2, 1000))
		s.Run()
		for _, v := range r.Report.Requests {
			v.ProgramID = ""
		}
		return r.Report.Requests
	}
	if !reflect.DeepEqual(run(true), run(false)) {
		t.Fatal("request MLFQ depends on family IDs")
	}
}
func TestResearchDefaultReportAbsent(t *testing.T) {
	data, err := json.Marshal(MetricsOutput{})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err = json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if _, ok := out["research_scheduler"]; ok {
		t.Fatal("default output changed")
	}
}
func TestResearchRejectUnsupportedEngine(t *testing.T) {
	cfg := msConfig(10000)
	cfg.ResearchScheduler = feedbackRuntime(t, "atlas")
	cfg.KVCPUBlocks = 10
	cfg.KVTransferBandwidth = 1
	_, err := NewSimulator(cfg, MustNewKVStoreFromConfig(cfg.KVCacheConfig), cfg.LatencyModel)
	if err == nil {
		t.Fatal("offload must be refused")
	}
}

func TestResearchBottomQueueMakesProgressThroughLargePrefills(t *testing.T) {
	runtime := feedbackRuntime(t, "mlfq")
	runtime.Report.Config.QuantaMS = []float64{1, 2}
	runtime.Report.Config.StarvationRatio = 100000
	s := feedbackSim(t, runtime, 1, 8)
	s.Horizon = 1000000
	s.InjectArrival(feedbackRequest("large-a", "a", 512, 2, 0))
	s.InjectArrival(feedbackRequest("large-b", "b", 512, 2, 0))
	for i := 0; i < 8; i++ {
		s.InjectArrival(feedbackRequest(fmt.Sprintf("short-%d", i), "short", 1, 1, int64(5000+i*3000)))
	}
	s.Run()
	if s.Metrics.CompletedRequests != 10 || s.Metrics.TotalOutputTokens != 12 {
		t.Fatalf("lost progress: completed=%d output=%d", s.Metrics.CompletedRequests, s.Metrics.TotalOutputTokens)
	}
	if runtime.Report.Requests["large-a"].Displacements == 0 {
		t.Fatal("fixture did not interrupt prefill")
	}
	if runtime.Report.Requests["large-a"].Displacements > 12 || runtime.Report.Requests["large-b"].Displacements > 12 {
		t.Fatal("bottom-level expiry destructively rotates")
	}
}

func TestResearchMemoryPressureRetainsExactOutput(t *testing.T) {
	runtime := feedbackRuntime(t, "mlfq")
	runtime.Report.Config.QuantaMS = []float64{1000, 2000}
	cfg := msConfig(1000000)
	cfg.KVCacheConfig = NewKVCacheConfig(8, 16, 0, 0, 0, 0)
	cfg.BatchConfig = NewBatchConfig(2, 32, 16, WithPrefixCachingDisabled(true))
	cfg.ResearchScheduler = runtime
	cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
	s := mustNewSimulator(t, cfg)
	s.InjectArrival(feedbackRequest("a", "p", 16, 60, 0))
	s.InjectArrival(feedbackRequest("b", "q", 16, 60, 0))
	s.Run()
	if s.Metrics.CompletedRequests != 2 || s.Metrics.TotalOutputTokens != 120 || s.Metrics.TotalInputTokens != 32 {
		t.Fatalf("memory preemption lost work: completed=%d output=%d", s.Metrics.CompletedRequests, s.Metrics.TotalOutputTokens)
	}
	stats := runtime.Snapshot().Requests
	if stats["a"].MemoryPreemptions+stats["b"].MemoryPreemptions == 0 {
		t.Fatal("fixture did not exercise memory preemption")
	}
	if stats["a"].Displacements+stats["b"].Displacements != 0 {
		t.Fatal("memory eviction mislabeled policy displacement")
	}
	if s.KVCache.UsedBlocks() != 0 {
		t.Fatal("KV leaked")
	}
}

func TestResearchQueuedPriorityReclaimsKVOnlyWhenNeeded(t *testing.T) {
	for _, mode := range []string{"mlfq", "atlas"} {
		for _, caching := range []bool{false, true} {
			for _, blocks := range []int64{2, 3} {
				t.Run(fmt.Sprintf("%s/cache=%t/blocks=%d", mode, caching, blocks), func(t *testing.T) {
					runtime, err := NewResearchSchedulerRuntime(mode, ResearchSchedulerConfig{QuantaMS: []float64{1, 2}, StarvationRatio: 1000})
					if err != nil {
						t.Fatal(err)
					}
					cfg := msConfig(1000000)
					cfg.KVCacheConfig = NewKVCacheConfig(blocks, 16, 0, 0, 0, 0)
					cfg.BatchConfig = NewBatchConfig(2, 32, 16, WithPrefixCachingDisabled(!caching))
					cfg.ResearchScheduler = runtime
					cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
					s := mustNewSimulator(t, cfg)
					s.InjectArrival(feedbackRequest("long", "long-family", 16, 10, 0))
					s.InjectArrival(feedbackRequest("short", "short-family", 1, 1, 2500))
					s.Run()
					stats := runtime.Snapshot().Requests["long"]
					if (stats.Displacements > 0) != (blocks == 2) {
						t.Fatalf("unexpected KV displacement: %+v", stats)
					}
					if s.Metrics.RequestCompletionTimes["short"] >= s.Metrics.RequestCompletionTimes["long"] {
						t.Fatal("lower-priority KV holder blocked short request until completion")
					}
					if s.Metrics.CompletedRequests != 2 || s.Metrics.TotalInputTokens != 17 || s.Metrics.TotalOutputTokens != 11 {
						t.Fatal("KV displacement changed logical work")
					}
					if stats.MemoryPreemptions != 0 || s.Metrics.RequestTTFTs["long"] != 1000 || s.KVCache.UsedBlocks() != 0 {
						t.Fatalf("KV displacement corrupted accounting: %+v", stats)
					}
				})
			}
		}
	}
}

func TestResearchCachedAdmissionsShareTokenBudget(t *testing.T) {
	runtime := feedbackRuntime(t, "atlas")
	cfg := msConfig(100000)
	cfg.BatchConfig = NewBatchConfig(2, 32, 0)
	cfg.ResearchScheduler = runtime
	cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
	s := mustNewSimulator(t, cfg)
	s.InjectArrival(feedbackRequest("warm", "warm", 64, 1, 0))
	s.InjectArrival(feedbackRequest("a", "a", 64, 1, 3000))
	s.InjectArrival(feedbackRequest("b", "b", 64, 1, 3000))
	s.Run()
	if s.Metrics.CompletedRequests != 3 || s.Metrics.TotalInputTokens != 192 || s.Metrics.TotalOutputTokens != 3 {
		t.Fatal("cache changed logical work")
	}
	if s.Metrics.RequestCompletionTimes["a"] != s.Metrics.RequestCompletionTimes["b"] {
		t.Fatal("cached admissions did not share one32-token batch")
	}
	if s.KVCache.CacheHitRate() <= 0 {
		t.Fatal("fixture did not hit cache")
	}
	for _, id := range []string{"a", "b"} {
		if runtime.Report.Requests[id].ModelServiceUs != 1000 {
			t.Fatalf("cached request %s charged uncached work", id)
		}
	}
}

func TestResearchCanceledPrefillCannotSupplyCachedTail(t *testing.T) {
	runtime, err := NewResearchSchedulerRuntime("mlfq", ResearchSchedulerConfig{QuantaMS: []float64{1, 2}, StarvationRatio: 1000})
	if err != nil {
		t.Fatal(err)
	}
	cfg := msConfig(1000000)
	cfg.KVCacheConfig = NewKVCacheConfig(5, 16, 0, 0, 0, 0)
	cfg.BatchConfig = NewBatchConfig(2, 32, 16)
	cfg.ResearchScheduler = runtime
	cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
	s := mustNewSimulator(t, cfg)
	long := feedbackRequest("long", "long", 79, 1, 0)
	short := feedbackRequest("short", "short", 65, 1, 500)
	s.EnqueueRequest(long)
	s.Step(0)
	s.Clock = 1000
	s.EnqueueRequest(short)
	s.scheduleBatch(1000)
	if got := len(s.KVCache.GetCachedBlocks(long.InputTokens)); got != 2 {
		t.Fatalf("unexecuted tail became a prefix hit: %d blocks", got)
	}
	if s.Metrics.PrefixCacheHitTokens != 32 || short.NumNewTokens != 16 {
		t.Fatalf("short call received canceled-work credit: hits=%d work=%d", s.Metrics.PrefixCacheHitTokens, short.NumNewTokens)
	}
	if runtime.Report.Requests["long"].Displacements != 1 {
		t.Fatal("fixture did not displace the lower-priority allocation")
	}
	s.executeBatchStep(1000)
	if got := len(s.KVCache.GetCachedBlocks(short.InputTokens)); got != 3 {
		t.Fatalf("executed prefix not published: %d blocks", got)
	}

	runtime, _ = NewResearchSchedulerRuntime("mlfq", runtime.Report.Config)
	cfg.ResearchScheduler = runtime
	s = mustNewSimulator(t, cfg)
	s.InjectArrival(feedbackRequest("long", "long", 79, 1, 0))
	s.InjectArrival(feedbackRequest("short", "short", 65, 1, 500))
	s.Run()
	if s.Metrics.CompletedRequests != 2 || s.Metrics.TotalInputTokens != 144 || s.Metrics.TotalOutputTokens != 2 {
		t.Fatal("canceled prefill changed logical work")
	}
	if s.Metrics.KVAllocationFailures != 0 {
		t.Fatal("fixture had KV allocation failures")
	}
	if runtime.Report.Requests["short"].ModelServiceUs != 2000 {
		t.Fatalf("short call skipped uncomputed work: %+v", runtime.Report.Requests["short"])
	}
}

func TestResearchPrefixRegistrationDiffersFromNativeSameBatchReuse(t *testing.T) {
	for _, feedback := range []bool{false, true} {
		cfg := msConfig(100000)
		cfg.BatchConfig = NewBatchConfig(2, 128, 0)
		if feedback {
			cfg.ResearchScheduler = feedbackRuntime(t, "mlfq")
		}
		cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
		s := mustNewSimulator(t, cfg)
		s.InjectArrival(feedbackRequest("a", "a", 64, 1, 0))
		s.InjectArrival(feedbackRequest("b", "b", 64, 1, 0))
		s.Run()
		wantHits := int64(48)
		if feedback {
			wantHits = 0
		}
		if s.Metrics.PrefixCacheHitTokens != wantHits {
			t.Fatalf("feedback=%t hits=%d want=%d", feedback, s.Metrics.PrefixCacheHitTokens, wantHits)
		}
		if s.Metrics.CompletedRequests != 2 || s.Metrics.TotalInputTokens != 128 || s.Metrics.TotalOutputTokens != 2 {
			t.Fatal("registration timing changed logical work")
		}
	}
}

func TestResearchCachedPreemptionPreservesTokens(t *testing.T) {
	runtime := feedbackRuntime(t, "mlfq")
	cfg := msConfig(1000000)
	cfg.BatchConfig = NewBatchConfig(1, 32, 0)
	cfg.ResearchScheduler = runtime
	cfg.LatencyModel = newFakeLatencyWith(testutil.FakeLatency{BaseTicks: 1000})
	s := mustNewSimulator(t, cfg)
	s.InjectArrival(feedbackRequest("warm", "warm", 128, 1, 0))
	s.InjectArrival(feedbackRequest("long", "p", 128, 20, 5000))
	s.InjectArrival(feedbackRequest("short", "q", 128, 1, 7500))
	s.Run()
	stats := runtime.Snapshot().Requests["long"]
	if stats.Displacements == 0 || stats.Resumes == 0 {
		t.Fatal("cache-on fixture did not preempt")
	}
	if s.Metrics.CompletedRequests != 3 || s.Metrics.TotalInputTokens != 384 || s.Metrics.TotalOutputTokens != 22 {
		t.Fatal("cached resume changed logical tokens")
	}
	if s.Metrics.RequestTTFTs["long"] != 1000 {
		t.Fatalf("cached resume changed initialTTFT: %v", s.Metrics.RequestTTFTs["long"])
	}
	if stats.RecomputedTokens >= 128 {
		t.Fatalf("cached tokens billed as recomputation: %d", stats.RecomputedTokens)
	}
}
